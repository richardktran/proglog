package utils

import (
	"fmt"
	"net"
)

func GetFreePort(host string, numberOfPorts int) ([]int, error) {
	listeners := make([]net.Listener, 0, numberOfPorts)
	ports := make([]int, 0, numberOfPorts)

	for i := 0; i < numberOfPorts; i++ {
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:0", host))
		if err != nil {
			// Close all listeners on error to prevent resource leaks
			for _, l := range listeners {
				l.Close()
			}
			return nil, err
		}

		// Collect listener and port
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}

	// Close all listeners after collecting all ports
	for _, listener := range listeners {
		listener.Close()
	}

	return ports, nil
}
