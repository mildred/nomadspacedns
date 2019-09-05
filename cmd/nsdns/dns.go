package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/miekg/dns"
)

const TTL = 60

var ShutdownTimeout time.Duration = 5 * time.Second
var RecursorTimeout time.Duration = 5 * time.Second

type Args struct {
	ConsulServer string
	Listen       string
	Domain       string
	ConsulDomain string
}

func stringEnv(name, defVal string) string {
	val, hasVal := os.LookupEnv(name)
	if !hasVal {
		val = defVal
	}
	return val
}

func main() {
	var args Args
	flag.StringVar(&args.ConsulServer, "consul-server", "127.0.0.1:8600", "Consul DNS server")
	flag.StringVar(&args.Listen, "listen", stringEnv("NSDNS_LISTEN_ADDR", "127.0.0.1:9653"), "Listen address [NSDNS_LISTEN_ADDR]")
	flag.StringVar(&args.Domain, "domain", stringEnv("NSDNS_DOMAIN", "ns-consul."), "Domain to serve [NSDNS_DOMAIN]")
	flag.StringVar(&args.ConsulDomain, "consul-domain", stringEnv("NSDNS_CONSUL_DOMAIN", "consul."), "Domain to recurse to consul [NSDNS_CONSUL_DOMAIN]")
	flag.Parse()

	if !strings.HasSuffix(args.Domain, ".") {
		args.Domain = args.Domain + "."
	}

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sig
		log.Printf("Received %v, terminating...", s)
		cancel()
		signal.Stop(sig)
	}()

	err := run(ctx, &args)
	if err != nil {
		log.Printf("Error: %v", err)
	}
}

func run(ctx context.Context, args *Args) error {
	var globalRecursors []string
	config, err := dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil {
		return fmt.Errorf("error parsing /etc/resolv.conf, %v", err)
	}
	for _, recursor := range config.Servers {
		if config.Port != "" {
			globalRecursors = append(globalRecursors, recursor+":"+config.Port)
		} else {
			globalRecursors = append(globalRecursors, recursor)
		}
	}

	client := &dns.Client{
		Net: "tcp",
	}
	handler := &Handler{
		Context:         ctx,
		RootDomain:      dns.SplitDomainName(args.Domain),
		Client:          client,
		Server:          args.ConsulServer,
		GlobalRecursors: globalRecursors,
		ConsulRecursors: []string{args.ConsulServer},
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(args.Domain, handler.ServeConsulNS)
	mux.HandleFunc(args.ConsulDomain, handler.ServeConsulRecursor)
	mux.HandleFunc(".", handler.ServeGlobalRecursor)

	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go runServer(ctx, &wg, "tcp", args, mux, errChan)
	go runServer(ctx, &wg, "udp", args, mux, errChan)
	wg.Wait()

	close(errChan)
	for e := range errChan {
		err = multierror.Append(err, e).ErrorOrNil()
	}

	return err
}

func runServer(ctx context.Context, wg *sync.WaitGroup, net string, args *Args, handler dns.Handler, e chan<- error) {
	defer wg.Done()
	server := &dns.Server{
		Addr:    args.Listen,
		Net:     net,
		Handler: handler,
	}

	go func() {
		<-ctx.Done()
		ctx2, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
		server.ShutdownContext(ctx2)
		defer cancel()
	}()

	log.Printf("Listening on %v %v...", server.Net, server.Addr)
	e <- server.ListenAndServe()
}

type Handler struct {
	Context         context.Context
	RootDomain      []string
	Client          *dns.Client
	Server          string
	GlobalRecursors []string
	ConsulRecursors []string
}

var ErrInvalid error = fmt.Errorf("Invalid domain")

func tail(elems []string) ([]string, string, error) {
	if len(elems) == 0 {
		return elems, "", ErrInvalid
	}
	return elems[:len(elems)-1], elems[len(elems)-1], nil
}

func (h *Handler) ServeGlobalRecursor(w dns.ResponseWriter, req *dns.Msg) {
	res := Recurse(h.Context, w.RemoteAddr(), req, h.GlobalRecursors)
	if err := w.WriteMsg(res); err != nil {
		log.Printf("[WARN] dns: failed to respond: %v", err)
	}
}

func (h *Handler) ServeConsulRecursor(w dns.ResponseWriter, req *dns.Msg) {
	res := Recurse(h.Context, w.RemoteAddr(), req, h.ConsulRecursors)
	if err := w.WriteMsg(res); err != nil {
		log.Printf("[WARN] dns: failed to respond: %v", err)
	}
}

func Recurse(ctx context.Context, raddr net.Addr, req *dns.Msg, recursors []string) *dns.Msg {
	q := req.Question[0]
	network := "udp"

	// Switch to TCP if the client is
	if _, ok := raddr.(*net.TCPAddr); ok {
		network = "tcp"
	}

	// Recursively resolve
	c := &dns.Client{Net: network, Timeout: RecursorTimeout}
	var r *dns.Msg
	var err error
	for _, recursor := range recursors {
		r, _, err = c.ExchangeContext(ctx, req, recursor)
		// Check if the response is valid and has the desired Response code
		if r != nil && (r.Rcode != dns.RcodeSuccess && r.Rcode != dns.RcodeNameError) {
			// If we still have recursors to forward the query to,
			// we move forward onto the next one else the loop ends
			continue
		} else if err == nil {
			// Compress the response; we don't know if the incoming
			// response was compressed or not, so by not compressing
			// we might generate an invalid packet on the way out.
			r.Compress = true

			// Forward the response
			return r
		}
		log.Printf("[ERR] dns: recurse failed: %v", err)
	}

	// If all resolvers fail, return a SERVFAIL message
	log.Printf("[ERR] dns: all resolvers failed for %v from client %s (%s)",
		q, raddr.String(), raddr.Network())
	return ErrorResponse(req, dns.RcodeServerFailure)
}

func ErrorResponse(req *dns.Msg, rcode int) *dns.Msg {
	m := &dns.Msg{}
	m.SetReply(req)
	m.Compress = true
	m.RecursionAvailable = true
	m.SetRcode(req, rcode)
	if edns := req.IsEdns0(); edns != nil {
		setEDNS(req, m, true)
	}
	return m
}

func (h *Handler) ServeConsulNS(w dns.ResponseWriter, req *dns.Msg) {
	ctx := h.Context
	resp := new(dns.Msg)
	resp.SetReply(req)
	resp.Compress = true
	resp.Authoritative = true
	resp.RecursionAvailable = false

	var err error
	for _, q := range req.Question {
		var ns, dc, service string
		elements := dns.SplitDomainName(q.Name)
		elements = elements[:len(elements)-len(h.RootDomain)]
		invalid := false

		elements, ns, err = tail(elements)
		if err != nil {
			invalid = true
		}

		for !invalid {
			var tag string
			elements, tag, err = tail(elements)
			if err != nil {
				break
			}
			switch tag {
			case "dc":
				elements, dc, err = tail(elements)
				if err != nil {
					invalid = true
				}
			case "service":
				elements, service, err = tail(elements)
				if err != nil {
					invalid = true
				}
				break
			default:
				invalid = true
			}
		}

		var domain string

		if service == "" || invalid {
			resp = ErrorResponse(req, dns.RcodeNameError)
			break
		} else {
			elements = append(elements, ns+"-"+service, "service")
			if dc != "" {
				elements = append(elements, dc)
			}
			elements = append(elements, "consul")

			domain = strings.Join(elements, ".") + "."
		}

		resp.Answer = append(resp.Answer, &dns.CNAME{
			Hdr: dns.RR_Header{
				Name:   q.Name,
				Rrtype: dns.TypeCNAME,
				Class:  q.Qclass,
				Ttl:    uint32(TTL / time.Second),
			},
			Target: domain,
		})

		recReq := new(dns.Msg)
		recReq.SetQuestion(domain, q.Qtype)
		recReq.Question[0].Qclass = q.Qclass

		recRes := Recurse(ctx, w.RemoteAddr(), recReq, h.ConsulRecursors)
		if recRes.Rcode == dns.RcodeSuccess {
			resp.Answer = append(resp.Answer, recRes.Answer...)
		}
	}

	w.WriteMsg(resp)
}

// setEDNS is used to set the responses EDNS size headers and
// possibly the ECS headers as well if they were present in the
// original request
func setEDNS(request *dns.Msg, response *dns.Msg, ecsGlobal bool) {
	// Enable EDNS if enabled
	if edns := request.IsEdns0(); edns != nil {
		// cannot just use the SetEdns0 function as we need to embed
		// the ECS option as well
		ednsResp := new(dns.OPT)
		ednsResp.Hdr.Name = "."
		ednsResp.Hdr.Rrtype = dns.TypeOPT
		ednsResp.SetUDPSize(edns.UDPSize())

		// Setup the ECS option if present
		if subnet := ednsSubnetForRequest(request); subnet != nil {
			subOp := new(dns.EDNS0_SUBNET)
			subOp.Code = dns.EDNS0SUBNET
			subOp.Family = subnet.Family
			subOp.Address = subnet.Address
			subOp.SourceNetmask = subnet.SourceNetmask
			if c := response.Rcode; ecsGlobal || c == dns.RcodeNameError || c == dns.RcodeServerFailure || c == dns.RcodeRefused || c == dns.RcodeNotImplemented {
				// reply is globally valid and should be cached accordingly
				subOp.SourceScope = 0
			} else {
				// reply is only valid for the subnet it was queried with
				subOp.SourceScope = subnet.SourceNetmask
			}
			ednsResp.Option = append(ednsResp.Option, subOp)
		}

		response.Extra = append(response.Extra, ednsResp)
	}
}

func ednsSubnetForRequest(req *dns.Msg) *dns.EDNS0_SUBNET {
	// IsEdns0 returns the EDNS RR if present or nil otherwise
	edns := req.IsEdns0()

	if edns == nil {
		return nil
	}

	for _, o := range edns.Option {
		if subnet, ok := o.(*dns.EDNS0_SUBNET); ok {
			return subnet
		}
	}

	return nil
}
