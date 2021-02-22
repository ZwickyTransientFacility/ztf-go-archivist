package stream

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumerLag returns the total lag of a consumer group's subscription to
// topics, summed across each of the topic's partitions. If the group has no
// committed offsets for the topic, then that topic will be excluded from the
// resulting map.
func ConsumerLag(ctx context.Context, brokerAddr string, consumerGroup string, topics []string) (map[string]int64, error) {
	addr, err := resolveBrokerAddr(brokerAddr)
	if err != nil {
		return nil, err
	}

	// Get the list of partitions
	conn, err := kafka.DialContext(ctx, "tcp", addr.String())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topics...)
	if err != nil {
		return nil, err
	}

	// Get the group's offset for each topic-partition
	c := kafka.Client{Addr: addr, Timeout: 10 * time.Second}

	groupReq := &kafka.OffsetFetchRequest{
		GroupID: consumerGroup,
		Topics:  make(map[string][]int),
	}
	for _, p := range partitions {
		groupReq.Topics[p.Topic] = append(groupReq.Topics[p.Topic], p.ID)
	}
	groupOffsets, err := c.OffsetFetch(ctx, groupReq)
	if err != nil {
		return nil, err
	}

	// Get the broker's offsets for each topic-partition
	offsetReq := &kafka.ListOffsetsRequest{
		Topics: make(map[string][]kafka.OffsetRequest),
	}
	for _, p := range partitions {
		offsetReq.Topics[p.Topic] = append(offsetReq.Topics[p.Topic], kafka.LastOffsetOf(p.ID))
	}
	brokerOffsets, err := c.ListOffsets(ctx, offsetReq)
	if err != nil {
		return nil, err
	}

	// Compare the broker values vs the group values
	result := make(map[string]int64)
	for topic, groupPos := range groupOffsets.Topics {
		committedByPartition := make(map[int]int64)
		latestByPartition := make(map[int]int64)
		for _, partition := range groupPos {
			committedByPartition[partition.Partition] = partition.CommittedOffset
		}
		for _, partition := range brokerOffsets.Topics[topic] {
			latestByPartition[partition.Partition] = partition.LastOffset
		}

		for partition, latest := range latestByPartition {
			result[topic] = latest - committedByPartition[partition]
		}
	}

	return result, nil
}
