package event

type EventHandlerMethod func(e *Event) error

type EventHandler struct {
	handlerDefault EventHandlerMethod
	handlers       map[string]EventHandlerMethod

	oneshotDefault EventHandlerMethod
	handlersOnce   map[string]EventHandlerMethod
}

func NewEventHandler() (eh *EventHandler) {
	eh = new(EventHandler)
	eh.handlers = make(map[string]EventHandlerMethod)
	return
}

// Process executes a matching handler for an event.
func (eh *EventHandler) Process(e *Event) error {
	if handler := eh.GetHandler(e.GetID()); handler != nil {
		return handler(e)
	}
	if eh.handlerDefault != nil {
		return eh.handlerDefault(e)
	}
	return ErrorEventHandlerNoMatch(e.GetID())
}

// Handle assigns a Go function to receive events of the specified IDs.
// The method receives one event at a time, but can be in any order and must be safe for multiple concurrent calls.
// Specifying no event IDs will assign the method to be the default event handler.
func (eh *EventHandler) Handle(method EventHandlerMethod, eventID ...string) *EventHandler {
	if len(eventID) > 0 {
		for i := 0; i < len(eventID); i++ {
			id := eventID[i]
			if id == "" {
				panic("EventHandler: Handle: eventID must never be empty string")
			}
			eh.handlers[id] = method
		}
	} else {
		eh.handlerDefault = method
	}
	return eh
}

// Oneshot returns a wrapper over the given method that will unregister itself after being processed once.
func (eh *EventHandler) Oneshot(method EventHandlerMethod) EventHandlerMethod {
	return func(e *Event) error {
		err := method(e)
		eh.Unhandle(e.GetID())
		return err
	}
}

// Unhandle removes one or more handlers. Specifying no event IDs will remove the default event handler.
func (eh *EventHandler) Unhandle(eventID ...string) *EventHandler {
	if len(eventID) > 0 {
		for i := 0; i < len(eventID); i++ {
			id := eventID[i]
			if id == "" {
				panic("EventHandler: Unhandle: eventID must never be empty string")
			}
			delete(eh.handlers, id)
		}
	} else {
		eh.handlerDefault = nil
	}
	return eh
}

// GetEventIDs returns the list of handled event IDs.
func (eh *EventHandler) GetEventIDs() []string {
	eventIDs := make([]string, 0)
	for eventID := range eh.handlers {
		eventIDs = append(eventIDs, eventID)
	}
	return eventIDs
}

// GetHandler returns a handler for an event ID.
func (eh *EventHandler) GetHandler(eventID string) EventHandlerMethod {
	handler, exists := eh.handlers[eventID]
	if exists {
		return handler
	}
	return nil
}

// GetHandlersMapClone creates a clone of the handler mapping and returns the clone. It is up to the caller to dereference the clone.
func (eh *EventHandler) GetHandlersMapClone() map[string]EventHandlerMethod {
	handlers := make(map[string]EventHandlerMethod)
	for eventID, handler := range eh.handlers {
		handlers[eventID] = handler
	}
	return handlers
}
