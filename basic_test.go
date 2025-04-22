package main

import (
    "io"
    "fmt"
    "testing"
    "net/http"
    // "net/http/httptest"
    "strings"
    "os"
    "os/exec"
    re "regexp"
    "time"
    "sync/atomic"
    "errors"
    svix "github.com/svix/svix-webhooks/go"
)

const (
    svixEndpointURLName = "SVIX_ENDPOINT_URL"
    endpointAddr = "localhost:8081"
    svixRouterAddr = "127.0.0.1:8080"
    svixRouterAddrPost = "http://localhost:8080"
    msgContentType = "application/json"
    msgCount = 5
    totalTimeout = time.Second * 2
)

var (
    urlRE = re.MustCompile("https://.+\\n")
)

/*
func genericCheck(r *http.Request) (bool, int) {
    w := httptest.NewRecorder()

    GenericHandle(w, r)
    res := w.Result();
    return res.StatusCode == http.StatusOK, res.StatusCode;
}

func TestBasicPing(t *testing.T) {
    t.Run("BasicPing_sub", func (t *testing.T) {
        bodyStr := "{ \"msg\": \"Hello world ! }"
        stringReader := strings.NewReader(bodyStr)
        req, _ := http.NewRequest(http.MethodPost, "/notifications", stringReader)
        req.Header.Add("Content-Type", "application/json")

        success, retCode := genericCheck(req)
        if !success {
            t.Errorf("Fail with response code %v", retCode)
            return
        }

        t.Log("Everything's fine now")
    }) // t.Run
}
*/

type LimitSignaledWriter struct {
    limit int 
    buf []byte
    done chan int
}

func (w *LimitSignaledWriter) Write(p []byte) (n int, err error) {
    w.buf = append(w.buf, p...)

    fmt.Println("Start write")
    if len(w.buf) >= w.limit && w.limit != -1 {
        fmt.Printf("\nWrite %v", len(w.buf))
        w.done <- 1
        w.limit = -1
    }
    fmt.Println("end write")

    return len(p), nil
}

func startCLIAndGetEndpointURL() *string {
    w := LimitSignaledWriter {
        limit: 100,
        buf: make([]byte, 0, 100),
        done: make(chan int),
    }
    cmd := exec.Command("svix", "listen", endpointAddr)
    cmd.Stdout = &w

    cmd.Start()
    <-w.done
    fmt.Println("Done getting output of svix cli")

    urlSlice := urlRE.Find(w.buf)

    if len(urlSlice) == 0 {
        return nil
    }
    str := string(urlSlice)
    return &str
}

func listenAndEcho(count int, addr string) chan error {
    var hasEchoedCount atomic.Uint32
    endCh := make(chan error, 2)

    s := http.Server {
        Addr: addr,
        Handler: HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            bytes, err := io.ReadAll(r.Body)

            if err != nil {
                endCh <- err
            } else {
                fmt.Printf("ECHO: %s\n", string(bytes))
            }

            if(hasEchoedCount.Add(1) >= msgCount) {
                endCh <- nil
            }
        }),
    }

    go s.ListenAndServe()
    return endCh
}

func TestBasicPing1(t *testing.T) {
    // starting cli listen and getting url
    endpointID := "testEndpointID"
    fmt.Println("start getting")
    endpointURL := os.Getenv(svixEndpointURLName)

    if endpointURL == "" {
        t.Fatalf("No \"%v\" set as env var", svixEndpointURLName)
    }

    // Starting our router
    s := http.Server {
        Addr: svixRouterAddr,
    };
    svixRouter := NewSvixRouter()
    go svixRouter.ListenAndServe(&s)
    defer svixRouter.BlockingStop()

    // Creating endpoint with provided url
    SvixClient.Endpoint.Create(nil, SvixAppUid, svix.EndpointIn{
        Uid: &endpointID,
        Url: endpointURL,
    }, nil)
    defer SvixClient.Endpoint.Delete(nil, SvixAppUid, endpointID)

    // Start listening up to 'msgCount' msgs
    stopEchoCh := listenAndEcho(msgCount, endpointAddr)

    payloadFmt := `{
        "EventType": "test.basic",
        "payload": {
            "value": %v
        }
    }`

    // Start sending
    go func() {
        for i := 0; i < msgCount; i++ {
            payload := fmt.Sprintf(payloadFmt, i)
            res, err := http.Post("http://" + svixRouterAddr, msgContentType, strings.NewReader(payload))
            t.Logf("StatusCode: %v", res.StatusCode)

            if res.StatusCode < 200 || res.StatusCode >= 300 {
                stopEchoCh <- errors.New(fmt.Sprintf("during sending: bad status code %v", res.StatusCode))
            } else if err != nil {
                stopEchoCh <- errors.New(fmt.Sprintf("during sending: %s", err.Error()))
            } else if i + 1 == msgCount {
                stopEchoCh <- nil
            }
        }
    }()

    // Waiting for timeout or echoing all incoming messages
    timeoutCh := time.After(totalTimeout)

    select {
    case err := <-stopEchoCh:
        if err != nil {
            t.Fatalf("ERROR: echoing: %s", err.Error())
        }
        break
    case <-timeoutCh:
        t.Fatal("Reached timeout")
    }
}
