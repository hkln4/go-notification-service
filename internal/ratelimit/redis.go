package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// reserveScript books the next GCRA slot atomically.
//
// State per key is one number: the theoretical arrival time (TAT) of the
// next free slot, in microseconds. The script reads it, books
// max(TAT, now) if that is within max_wait, and moves TAT one interval on.
//
// Time comes from Redis (TIME), not from the workers, so clock skew between
// worker hosts cannot let two of them book the same slot. Since Redis 5,
// scripts replicate their effects rather than the script itself, so a
// non-deterministic command like TIME is allowed before a write.
//
// Microseconds since the epoch (~1.8e15) fit exactly in a Lua double.
//
// KEYS[1] = limiter key
// ARGV[1] = interval between slots, µs
// ARGV[2] = longest acceptable wait, µs
// Returns {1, slot_µs, wait_µs} when booked, {0, wait_µs} when not.
var reserveScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local max_wait = tonumber(ARGV[2])

local tat = tonumber(redis.call('GET', KEYS[1]))
if tat == nil or tat < now then
  tat = now
end

local wait = tat - now
if wait > max_wait then
  return {0, wait}
end

local next_tat = tat + interval
-- Keep the key only while it matters; an idle limiter costs nothing.
redis.call('SET', KEYS[1], next_tat, 'PX', math.ceil((next_tat - now) / 1000) + 1000)
return {1, tat, wait}
`)

// Redis is the shared limiter used by every worker.
type Redis struct {
	client   redis.Scripter
	interval time.Duration
	prefix   string
}

// NewRedis allows perSecond slots per key.
func NewRedis(client redis.Scripter, perSecond int) *Redis {
	return &Redis{client: client, interval: time.Second / time.Duration(perSecond), prefix: "ratelimit:"}
}

func (r *Redis) Wait(ctx context.Context, key string) (time.Time, error) {
	maxWait := budget(ctx, time.Now())
	if maxWait <= 0 {
		return time.Time{}, ErrWaitTooLong
	}
	res, err := reserveScript.Run(ctx, r.client, []string{r.prefix + key},
		r.interval.Microseconds(), maxWait.Microseconds()).Int64Slice()
	if err != nil {
		if ctx.Err() != nil {
			return time.Time{}, ctx.Err()
		}
		return time.Time{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if len(res) < 2 || res[0] == 0 {
		return time.Time{}, ErrWaitTooLong
	}
	slot := time.UnixMicro(res[1])
	if err := sleepUntil(ctx, time.Duration(res[2])*time.Microsecond); err != nil {
		return time.Time{}, err
	}
	return slot, nil
}
