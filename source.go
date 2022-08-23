package datapipe

type Params map[string]interface{}

type DataList[T any] interface {
	GetData() []T
}

type DataSource[T any, K comparable] interface {
	GetList(params Params) DataList[T]
	GetItem(key K) (T, error)
	Store(item T) error
	Delete(key K) error
}
