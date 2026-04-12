package grouprabbitmq

import "fmt"

// Sequence key matches node/src/store/GroupStateStore sequenceKey with queuePrefix namespace.
func sequenceKey(queuePrefix, groupID string) string {
	ns := ""
	if queuePrefix != "" {
		ns = queuePrefix + ":"
	}
	return fmt.Sprintf("%sgroup:%s:sequence", ns, groupID)
}
