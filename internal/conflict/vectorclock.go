package conflict

import (
	"fmt"
	"time"
)

type VectorClockResolver struct {
	fallback ConflictResolver // used when concurrent
}

func NewVectorClockResolver(fallback ConflictResolver) *VectorClockResolver {
	if fallback == nil {
		fallback = NewLWWResolver()
	}
	return &VectorClockResolver{fallback: fallback}
}

func (r *VectorClockResolver) Type() ResolverType {
	return ResolverVectorClock
}

func (r *VectorClockResolver) Resolve(c *Conflict) *Resolution {
	localVC := c.Local.VClock
	remoteVC := c.Remote.VClock

	if localVC == nil && remoteVC == nil {
		// fall back to LWW
		return r.fallback.Resolve(c)
	}
	if localVC == nil {
		return &Resolution{
			ConflictID:   c.ID,
			Winner:       c.Remote,
			ResolverType: ResolverVectorClock,
			Reason:       "local_has_no_vclock",
			ResolvedAt:   time.Now(),
		}
	}
	if remoteVC == nil {
		return &Resolution{
			ConflictID:   c.ID,
			Winner:       c.Local,
			ResolverType: ResolverVectorClock,
			Reason:       "remote_has_no_vclock",
			ResolvedAt:   time.Now(),
		}
	}

	if remoteVC.HappensBefore(localVC) {
		return &Resolution{
			ConflictID:   c.ID,
			Winner:       c.Local,
			ResolverType: ResolverVectorClock,
			Reason:       fmt.Sprintf("remote_happens_before_local: local=%s remote=%s", localVC, remoteVC),
			ResolvedAt:   time.Now(),
		}
	}
	if localVC.HappensBefore(remoteVC) {
		return &Resolution{
			ConflictID:   c.ID,
			Winner:       c.Remote,
			ResolverType: ResolverVectorClock,
			Reason:       fmt.Sprintf("local_happens_before_remote: local=%s remote=%s", localVC, remoteVC),
			ResolvedAt:   time.Now(),
		}
	}

	// Concurrent — fallback to LWW
	res := r.fallback.Resolve(c)
	res.Reason = fmt.Sprintf("concurrent_fallback_to_%s: %s", r.fallback.Type(), res.Reason)
	return res
}
