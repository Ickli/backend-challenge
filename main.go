package main

import (
    // "fmt"
    "net/http"
    "log"
    "io"
    "os"
    "strconv"
    "encoding/json"
    "hash/fnv"
    "context"
    "errors"
    "sync"
    "sync/atomic"
    "os/signal"
    "time"
    svix "github.com/svix/svix-webhooks/go"
)

const (
    maxMsgProcessed = 5
    maxRetryCount = 5
    retryIntervalNs = time.Millisecond * 200
    acceptUrl = "/notifications"
    testUrl = "/webhooks"
    jsonContentType = "application/json"
    envSvixKeyName = "SVIX_KEY"
    envSvixAppUidName = "SVIX_APP_UID"
    jsonEventIdName = "EventId"
    jsonEventTypeName = "EventType"
    jsonPayloadName = "payload"
)

var (
    areSvixCredentialsFetched = false
    SvixKey = ""
    SvixAppUid = ""
    SvixClient *svix.Svix
)

type Msg struct {
    data map[string]any
    eventType string
    idempKey string

    retryCount uint16
}

func NewMsgFromJson(jsonStr string) (Msg, error) {
    deseredJson := make(map[string]any)

    err := json.Unmarshal([]byte(jsonStr), &deseredJson)
    if err != nil {
        return Msg{}, err
    }

    var ok bool
    var anyJsonKey any
    anyJsonKey, ok = deseredJson[jsonEventIdName]

    var idempKey string
    if ok /* is idempotency key pregenerated */ {
        idempKey = anyJsonKey.(string)
    } else {
        idempKey = generateIdempKey(jsonStr)
    }

    anyJsonKey, ok = deseredJson[jsonEventTypeName]
    if !ok {
        return Msg{}, errors.New("body has no \"EventType\" field");
    }
    eventType := anyJsonKey.(string)

    anyJsonKey, ok = deseredJson[jsonPayloadName]
    if !ok {
        return Msg{}, errors.New("body has no \"payload\" field");
    }
    payload := anyJsonKey.(map[string]any)

    msg := Msg{
        data: payload,
        eventType: eventType,
        idempKey: idempKey,
        retryCount: 0,
    }
    return msg, nil
}

func (msg *Msg) TrySendToSvix(tickets chan byte) {
    if !areSvixCredentialsFetched {
        log.Fatal("ERROR: try to send to svix, but credentials not fetched")
    }

    for ;msg.retryCount < maxRetryCount; msg.retryCount++ {
        if msg.retryCount > 0 {
            log.Printf("\twill retry in at least %v ns", retryIntervalNs)
            time.Sleep(retryIntervalNs)
        }

        ticket := <-tickets
        defer func() {tickets <- ticket}()
        
        _, err := SvixClient.Message.Create(context.Background(), SvixAppUid, svix.MessageIn{
            EventType: msg.eventType,
            Payload: msg.data,
        }, &svix.MessageCreateOptions{ IdempotencyKey: &msg.idempKey })

        if err == nil {
            return
        }

        if errors.Is(err, svix.Error{}) {
            log.Printf("ERROR: svix fail sending msg (%s)", err.Error())
        } else {
            log.Printf("ERROR: critical fail sending msg (%s)", err.Error())
            return
        }
    }

    log.Printf("ERROR: fail sending msg, retry amount exceeded")
}

func GenericHandleChanneled(w http.ResponseWriter , r *http.Request, msgChannel chan<- Msg, inGroup, sendGroup *sync.WaitGroup, quit *atomic.Bool) {
    inGroup.Add(1)
    defer inGroup.Done()

    if quit.Load() {
        return
    }

    actualType := r.Header["Content-Type"][0]

    if actualType != jsonContentType {
        log.Printf("ERROR: expected type \"%s\", got \"%s\"\n", jsonContentType, actualType)
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    if r.Method != http.MethodPost {
        log.Printf("ERROR: expected POST method, got %s\n", r.Method)
        w.WriteHeader(http.StatusBadRequest)
        return
    }

    byteBuf, err := io.ReadAll(r.Body)
    jsonStr := string(byteBuf)

    if err != nil {
        log.Printf("ERROR: %s\n", err.Error())
        w.WriteHeader(http.StatusInternalServerError)
        return
    }

    msg, err := NewMsgFromJson(jsonStr)
    if err != nil {
        log.Printf("ERROR: can't parse msg: %s", err.Error())
        w.WriteHeader(http.StatusBadRequest)
        return 
    }

    sendGroup.Add(1)
    msgChannel <- msg
}

func processPendingMsg(msgChannel <-chan Msg, toSend *sync.WaitGroup, tickets chan byte) {
    for {
        msg, ok := <-msgChannel
        if !ok {
            log.Print("Message channel closed, stop processing incoming msg");
            break;
        }
        go func() {
            msg.TrySendToSvix(tickets)
            toSend.Done()
        }()
    }
}

func fetchSvixCredentials() {
    if areSvixCredentialsFetched {
        log.Print("WARNING: svix credentials fetched twice")
        return
    }

    SvixKey = os.Getenv(envSvixKeyName)
    SvixAppUid = os.Getenv(envSvixAppUidName)
    if SvixKey == "" {
        log.Fatal("ERROR: empty svix key from env")
    }
    if SvixAppUid == "" {
        log.Fatal("ERROR: empty svix app uid from env")
    }

    areSvixCredentialsFetched = true
}

func generateIdempKey(str string) string {
    hash := fnv.New32a()
    hash.Write([]byte(str))
    return strconv.FormatUint(uint64(hash.Sum32()), 10)
}

func init() {
    fetchSvixCredentials()

    var err error
    SvixClient, err = svix.New(SvixKey, nil)

    if err != nil {
        log.Fatalf("ERROR: couldn't init svix connection: %s", err.Error())
    }
}

func makeTickets(count uint) chan byte {
    c := make(chan byte, count)
    for i := uint(0); i < count; i++ {
        c <- 0;
    }
    return c
}

type SvixRouter struct {
    msgChannel chan Msg
    tickets chan byte
    stopWork chan os.Signal
    stopWorkDone chan struct{}

    stopHandleIncoming atomic.Bool
    incomingGroup sync.WaitGroup
    sendingGroup sync.WaitGroup
    outError error
}

func NewSvixRouter() SvixRouter {
    return SvixRouter {
        msgChannel: make(chan Msg, 16),
        tickets: makeTickets(maxMsgProcessed),
        stopWork: make(chan os.Signal, 1),
        stopWorkDone: make(chan struct{}, 1),
    }
}

type HandlerFunc func(w http.ResponseWriter, req *http.Request)

func (f HandlerFunc) ServeHTTP(w http.ResponseWriter, req *http.Request) {
    f(w, req)
}

func (r *SvixRouter) ListenAndServe(server *http.Server) error {
    server.Handler = HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
        GenericHandleChanneled(w, req, r.msgChannel, &r.incomingGroup, &r.sendingGroup, &r.stopHandleIncoming)
    })

    signal.Notify(r.stopWork, os.Interrupt)

    log.Println("Start work")
    go processPendingMsg(r.msgChannel, &r.sendingGroup, r.tickets)
    go func() {
        r.outError = server.ListenAndServe()
    }()

    <-r.stopWork
    r.stop()
    r.stopWorkDone <- struct{}{}
    // log.Printf("End work with err: %v", r.outError)

    return r.outError
}

func (r *SvixRouter) stop() {
    r.stopHandleIncoming.Store(true)
    log.Println("Stop registering new msg, finish put remaining msg to buffer...")
    r.incomingGroup.Wait()

    close(r.msgChannel)
    log.Println("Wating for all msgs in buffer to be sent")
    r.sendingGroup.Wait()
    log.Println("End work")
}

func (r *SvixRouter) BlockingStop() {
    r.stopWork <- os.Interrupt
    <-r.stopWorkDone
}

func main() {
    s := http.Server{
        Addr: ":8080",
    }
    router := NewSvixRouter()
    router.ListenAndServe(&s)
}
