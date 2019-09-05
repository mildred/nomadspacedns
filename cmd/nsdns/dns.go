package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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

type Args struct {
	ConsulServer string
	Listen       string
	Domain       string
}

func main() {
	var args Args
	//flag.StringVar(&args.ConsulServer, "consul-server", "127.0.0.1:8600", "Consul DNS server")
	flag.StringVar(&args.Listen, "listen", "127.0.0.1:9653", "Listen address")
	flag.StringVar(&args.Domain, "domain", "ns-consul.", "Domain to serve")
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
	client := &dns.Client{
		Net: "tcp",
	}
	handler := &Handler{
		Context:    ctx,
		RootDomain: dns.SplitDomainName(args.Domain),
		Client:     client,
		Server:     args.ConsulServer,
	}
	mux := dns.NewServeMux()
	mux.Handle(args.Domain, handler)

	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go runServer(ctx, &wg, "tcp", args, mux, errChan)
	go runServer(ctx, &wg, "udp", args, mux, errChan)
	wg.Wait()

	close(errChan)
	var err error
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
	Context    context.Context
	RootDomain []string
	Client     *dns.Client
	Server     string
}

var ErrInvalid error = fmt.Errorf("Invalid domain")

func tail(elems []string) ([]string, string, error) {
	if len(elems) == 0 {
		return elems, "", ErrInvalid
	}
	return elems[:len(elems)-1], elems[len(elems)-1], nil
}

func (h *Handler) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
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
			domain = "consul."
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

		/*
			sub := new(dns.Msg)
			sub.SetQuestion(domain, q.Qtype)
			sub.Question[0].Qclass = q.Qclass

			subResp, _, err := h.Client.ExchangeContext(ctx, sub, h.Server)
			if err != nil {
				log.Printf("Error: %v", err)
				return
			}
		*/
	}

	w.WriteMsg(resp)
}
