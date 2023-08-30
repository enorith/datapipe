package datapipe

import (
	"fmt"

	"gorm.io/gorm"
)

const (
	DefaultPageSize = 20
)

type Params map[string]interface{}
type Scopes []func(*gorm.DB) *gorm.DB

func (p Params) WithPage(page, perPage int) Params {
	p["page"] = PageParam{
		Page:    page,
		PerPage: perPage,
	}

	return p
}

func (p Params) WithScopes(scopes ...func(db *gorm.DB) *gorm.DB) Params {
	if _, ok := p["scopes"]; !ok {
		p["scopes"] = make(Scopes, 0)
	}
	if sc, ok := p["scopes"].(Scopes); ok {
		sc = append(sc, scopes...)
		p["scopes"] = sc
	}

	return p
}

type PageParam struct {
	Page, PerPage int
}

type PageMeta struct {
	Page    int `json:"page"`
	PerPage int `json:"pre_page"`
	Total   int `json:"total"`
}

type DataList[T any] interface {
	GetData() []T
}

type PagenationDataList interface {
	GetPageMeta() PageMeta
}

type DataSource[T any, K comparable] interface {
	GetList(params Params) DataList[T]
	GetItem(key K) (T, error)
	Store(item *T) error
	Update(key K, item *T) error
	Delete(key K) error
}

type SimpleDataList[T any] struct {
	Items []T      `json:"items"`
	Meta  PageMeta `json:"meta"`
}

func (sd *SimpleDataList[T]) GetData() []T {
	return sd.Items
}

func NewSimpleDataList[T any](items []T) *SimpleDataList[T] {
	l := len(items)
	return NewSimpleDataPageList(items, PageMeta{
		Page:    1,
		PerPage: l,
		Total:   l,
	})
}

func NewSimpleDataPageList[T any](items []T, meta PageMeta) *SimpleDataList[T] {
	return &SimpleDataList[T]{Items: items, Meta: meta}
}

type DBSource[T any, K comparable] struct {
	db *gorm.DB
	pk string
}

func (ds *DBSource[T, K]) GetList(params Params) DataList[T] {
	items := make([]T, 0)
	tx := ds.db.Session(&gorm.Session{})

	if scs, ok := params["scopes"].(Scopes); ok {
		tx = tx.Scopes(scs...)
	}

	if page, exists := params["page"]; exists {
		if pr, ok := page.(PageParam); ok {
			meta, _ := ds.Pagination(tx, pr.Page, pr.PerPage, &items)
			return NewSimpleDataPageList(items, meta)
		}
	}

	tx.Find(&items)

	return NewSimpleDataList(items)
}

func (ds *DBSource[T, K]) Pagination(tx *gorm.DB, pageIndex int, pageSize int, dest interface{}, countField ...string) (meta PageMeta, err error) {
	if pageIndex < 1 {
		pageIndex = 1
	}
	if pageSize < 1 {
		pageSize = DefaultPageSize
	}
	meta.Page = pageIndex
	meta.PerPage = pageSize

	f := "*"
	if len(countField) > 0 {
		f = countField[0]
	}

	newTx := tx.Session(&gorm.Session{
		NewDB: true,
	})
	err = newTx.Select(fmt.Sprintf("COUNT(%s)", f)).Table("(?) as `aggragate`", tx).Scan(&meta.Total).Error
	if err != nil {
		return PageMeta{}, err
	}

	err = tx.Limit(pageSize).Offset(pageSize * (pageIndex - 1)).Find(dest).Error

	return
}
func (ds *DBSource[T, K]) GetItem(key K) (item T, err error) {
	conds := make([]interface{}, 0)
	if ds.pk != "" {
		conds = append(conds, fmt.Sprintf("%s = ?", ds.pk))
	}

	conds = append(conds, key)

	db := ds.db.Session(&gorm.Session{}).Find(&item, conds...)
	err = db.Error
	if db.RowsAffected == 0 {
		err = gorm.ErrRecordNotFound
	}

	return
}

func (ds *DBSource[T, K]) Store(item *T) error {
	return ds.db.Create(item).Error
}

func (ds *DBSource[T, K]) Update(key K, item *T) error {
	db := ds.db.Session(&gorm.Session{})
	if ds.pk != "" {
		db.Where(fmt.Sprintf("%s = ?", ds.pk))
	} else {
		db.Model(item)
	}

	return db.Updates(item).Error
}

func (ds *DBSource[T, K]) Delete(key K) error {
	var model T
	conds := make([]interface{}, 0)
	if ds.pk != "" {
		conds = append(conds, fmt.Sprintf("%s = ?", ds.pk))
	}

	conds = append(conds, key)

	return ds.db.Session(&gorm.Session{}).Delete(&model, conds...).Error
}

func NewDBDataSource[T any, K comparable](db *gorm.DB) *DBSource[T, K] {
	var model T

	return &DBSource[T, K]{db: db.Model(&model)}
}

func NewDBDataTableSource[T any, K comparable](db *gorm.DB, table string, pk ...string) *DBSource[T, K] {
	p := "id"
	if len(pk) > 0 {
		p = pk[0]
	}
	return &DBSource[T, K]{db: db.Table(table), pk: p}
}
