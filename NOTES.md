1. Listens on an HTTP port.
    Done. Just listening on a port.
2. Accepts JSON formatted POST messages on /notifications.
    Done. Accepting through GenericHandleChanneled()
3. Sends every received event to Svix as an individual message.
    Done
4. Ensures idempotency in case the same message is received multiple times.
    Svix ensures it itself. We just need to send msgs
5. Handles errors raised by Svix appropriately (and, in particular, handles any rate limit errors in a sensible way).
    Retries if it's svix's errors during sending. Logs error if something else
