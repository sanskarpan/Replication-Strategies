package conflict

import (
	"fmt"
	"strings"
	"time"
)

type LWWResolver struct{}

func NewLWWResolver() *LWWResolver {
	return &LWWResolver{}
}

func (r *LWWResolver) Type() ResolverType {
	return ResolverLWW
}

// Resolve picks the entry with the highest timestamp.
// On tie, use lexicographic comparison of NodeID as tiebreak.
func (r *LWWResolver) Resolve(c *Conflict) *Resolution {
	winner := c.Local
	reason := "local_newer"

	if c.Remote.Timestamp > c.Local.Timestamp {
		winner = c.Remote
		reason = "remote_newer"
	} else if c.Remote.Timestamp == c.Local.Timestamp {
		// tiebreak on nodeID (higher wins)
		if strings.Compare(c.Remote.NodeID, c.Local.NodeID) > 0 {
			winner = c.Remote
			reason = fmt.Sprintf("timestamp_tie_nodeID_tiebreak:remote=%s>local=%s", c.Remote.NodeID, c.Local.NodeID)
		} else {
			reason = fmt.Sprintf("timestamp_tie_nodeID_tiebreak:local=%s>=remote=%s", c.Local.NodeID, c.Remote.NodeID)
		}
	}

	return &Resolution{
		ConflictID:   c.ID,
		Winner:       winner,
		ResolverType: ResolverLWW,
		Reason:       reason,
		ResolvedAt:   time.Now(),
	}
}
