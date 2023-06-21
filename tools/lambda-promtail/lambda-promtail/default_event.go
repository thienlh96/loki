package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	// "github.com/aws/aws-lambda-go/events"
	"github.com/grafana/loki/pkg/logproto"
)

func parserEvent(ctx context.Context, b *batch, ev interface{}) error {
	// if err != nil {
	// 	return err
	// }
	if reflect.TypeOf(ev).Kind() == reflect.Interface {

	}
	json_data, err := json.Marshal(ev)
	if err != nil {
		println(err)
	}
	log_text := string(json_data)
	labels := parser_json(log_text)

	labels = applyExtraLabels(labels)

	timestamp := time.Now()

	if err := b.add(ctx, entry{labels, logproto.Entry{
		Line:      log_text,
		Timestamp: timestamp,
	}}); err != nil {
		return err
	}
	return nil
}

func processEvent(ctx context.Context, ev interface{}, pClient Client) error {
	batch, err := newBatch(ctx, pClient)
	if err != nil {
		return err
	}

	err = parserEvent(ctx, batch, ev)
	if err != nil {
		return fmt.Errorf("error parsing log event: %s", err)
	}

	err = pClient.sendToPromtail(ctx, batch)
	if err != nil {
		return err
	}

	return nil
}
