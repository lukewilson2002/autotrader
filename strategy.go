package autotrader

type Strategy interface {
	Init(t *Trader)
	Next(t *Trader)
}
