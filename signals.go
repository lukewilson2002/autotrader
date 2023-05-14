package autotrader

import "reflect"

type Signaler interface {
	SignalConnect(signal string, handler func(interface{})) error  // SignalConnect connects the handler to the signal.
	SignalConnected(signal string, handler func(interface{})) bool // SignalConnected returns true if the handler is connected to the signal.
	SignalConnections(signal string) []func(interface{})           // SignalConnections returns a slice of handlers connected to the signal.
	SignalDisconnect(signal string, handler func(interface{}))     // SignalDisconnect removes the handler from the signal.
	SignalEmit(signal string, data interface{})                    // SignalEmit emits the signal with the data.
}

type SignalManager struct {
	signalConnections map[string][]func(interface{})
}

func (s *SignalManager) SignalConnect(signal string, handler func(interface{})) error {
	if s.signalConnections == nil {
		s.signalConnections = make(map[string][]func(interface{}))
	}
	s.signalConnections[signal] = append(s.signalConnections[signal], handler)
	return nil
}

func (s *SignalManager) SignalConnected(signal string, handler func(interface{})) bool {
	if s.signalConnections == nil {
		return false
	}
	for _, h := range s.signalConnections[signal] {
		if reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer() {
			return true
		}
	}
	return false
}

func (s *SignalManager) SignalConnections(signal string) []func(interface{}) {
	if s.signalConnections == nil {
		return nil
	}
	return s.signalConnections[signal]
}

func (s *SignalManager) SignalDisconnect(signal string, handler func(interface{})) {
	if s.signalConnections == nil {
		return
	}
	for i, h := range s.signalConnections[signal] {
		if reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer() {
			s.signalConnections[signal] = append(s.signalConnections[signal][:i], s.signalConnections[signal][i+1:]...)
		}
	}
}

func (s *SignalManager) SignalEmit(signal string, data interface{}) {
	if s.signalConnections == nil {
		return
	}
	for _, handler := range s.signalConnections[signal] {
		handler(data)
	}
}
