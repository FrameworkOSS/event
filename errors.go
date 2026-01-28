package event

import "fmt"

func ErrorEventHandlerNoMatch(eventID string) error {
	return fmt.Errorf("EventHandler: no match: %s", eventID)
}
