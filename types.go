package event

// Event defines a structure representing something that occurred, such as (but not limited to):
// - Data queries
// - Responses to data queries
// - Global broadcasts by the portal or other features
// Fields should always be added, and NEVER removed or reordered, or else you must increment the API major version number!
type Event struct {
	portal       string   //The portal instance this event is associated with.
	id           string   //Unique identifier for this event type. Specifications should be agreed upon for common events.
	channel      string   //If necessary, an ID to continue tuning in to this event channel or to log this unique event. (ChannelSrc)
	producer     string   //The closest relative identifer for the producer of this event.
	participants []string //If necessary, the consumers which are exclusively allowed to hear this event. (ChannelDst)
	offsets      []uint64 //Byte addresses as pointers to index the data, but optional in case of private feature structures.
	data         []byte   //The actual values for the arguments.
	epochMilli   uint64   //The Unix epoch timestamp of this event's creation in milliseconds. Leave zero for the portal to auto-fill.
}
