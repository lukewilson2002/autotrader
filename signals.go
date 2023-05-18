package autotrader

import "reflect"

// Signaler is an interface for objects that can emit signals which fire event handlers. This is used to implement event-driven programming. Embed a pointer to a SignalManager in your struct to have signals entirely for free.
//
// Example:
//
//	type MyStruct struct {
//		*SignalManager // Now MyStruct has SignalConnect, SignalEmit, etc.
//	}
//
// When your type emits signals, they should be listed somewhere in the documentation. For example:
//
//	// Signals:
//	//  - MySignal() - Emitted when...
//	//  - ThingChanged(newThing *Thing) - Emitted when a thing changes.
//	type MyStruct struct { ... }
type Signaler interface {
	SignalConnect(signal string, identity any, handler func(...any), bindings ...any) error // SignalConnect connects the handler to the signal under identity.
	SignalConnected(signal string, identity any, handler func(...any)) bool                 // SignalConnected returns true if the handler under the identity is connected to the signal.
	SignalConnections(signal string) []SignalHandler                                        // SignalConnections returns a slice of handlers connected to the signal.
	SignalDisconnect(signal string, identity any, handler func(...any))                     // SignalDisconnect removes the handler under identity from the signal.
	SignalEmit(signal string, data ...any)                                                  // SignalEmit emits the signal with the data.
}

// SignalHandler wraps a signal handler.
type SignalHandler struct {
	Identity any          // Identity is used to identify functions implemented on the same type. It is typically a pointer to an object that owns the callback function, but it can be a string or any other type.
	Callback func(...any) // Callback is the function that is called when the signal is emitted.
	Bindings []any        // Bindings are arguments that are passed to the callback function when the signal is emitted. These are typically used to pass context.
}

// SignalManager is a struct that implements the Signaler interface. Embed this into your struct to have signals entirely for free. Emitting a signal will call all handlers connected to the signal, but if no handlers are connected then it is a no-op. This means signals are very cheap and only come at a cost when they're actually used.
type SignalManager struct {
	signalConnections map[string][]SignalHandler
}

// SignalConnect connects a callback function to the signal. The callback function will be called when the signal is emitted. The identity is used to identify functions implemented on the same type. It is typically a pointer to an object that owns the callback function, but it can be a string or any other type. Bindings are arguments that are passed to the callback function when the signal is emitted. These are typically used to pass context.
func (s *SignalManager) SignalConnect(signal string, identity any, callback func(...any), bindings ...any) error {
	if s.signalConnections == nil {
		s.signalConnections = make(map[string][]SignalHandler)
	}
	// Check if the callback and identity is already connected to the signal.
	if connections, ok := s.signalConnections[signal]; ok {
		for _, h := range connections {
			if h.Identity == identity && reflect.ValueOf(h.Callback).Pointer() == reflect.ValueOf(callback).Pointer() {
				return nil
			}
		}
	}
	s.signalConnections[signal] = append(s.signalConnections[signal], SignalHandler{identity, callback, bindings})
	return nil
}

// SignalConnected returns true if the callback function under the identity is connected to the signal.
func (s *SignalManager) SignalConnected(signal string, identity any, callback func(...any)) bool {
	if s.signalConnections == nil {
		return false
	}
	for _, h := range s.signalConnections[signal] {
		if h.Identity == identity && reflect.ValueOf(h.Callback).Pointer() == reflect.ValueOf(callback).Pointer() {
			return true
		}
	}
	return false
}

// SignalConnections returns a slice of handlers connected to the signal.
func (s *SignalManager) SignalConnections(signal string) []SignalHandler {
	if s.signalConnections == nil {
		return nil
	}
	return s.signalConnections[signal]
}

// SignalDisconnect removes the equivalent callback function under the identity from the signal.
func (s *SignalManager) SignalDisconnect(signal string, identity any, callback func(...any)) {
	if s.signalConnections == nil {
		return
	}
	connections := s.signalConnections[signal]
	for i, h := range connections {
		if h.Identity == identity && reflect.ValueOf(h.Callback).Pointer() == reflect.ValueOf(callback).Pointer() {
			s.signalConnections[signal] = append(connections[:i], connections[i+1:]...)
			break
		}
	}
}

// SignalEmit calls all handlers connected to the signal with the data. If no handlers are connected then it is a no-op.
func (s *SignalManager) SignalEmit(signal string, data ...any) {
	if s.signalConnections == nil {
		return
	}
	for _, handler := range s.signalConnections[signal] {
		args := make([]any, len(data)+len(handler.Bindings))
		copy(args, data)
		copy(args[len(data):], handler.Bindings)
		handler.Callback(args...)
	}
}
