package main

import "log"

type progressReport struct {
	nEvents int
}

func printProgress(updates chan progressReport) {
	for msg := range updates {
		log.Printf("received %d events\t", msg.nEvents)
	}
}
