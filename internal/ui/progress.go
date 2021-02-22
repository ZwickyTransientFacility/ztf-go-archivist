package ui

import "log"

type ProgressReport struct {
	NEvents int
}

func PrintProgress(updates chan ProgressReport) {
	for msg := range updates {
		log.Printf("received %d events\t", msg.NEvents)
	}
}
