package datapipe_test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/enorith/datapipe"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DSN = "root:root@tcp(localhost:3306)/test?charset=utf8mb4&parseTime=True&loc=Local"

var (
	source      datapipe.DataSource[User, int64]
	sourceTable datapipe.DataSource[map[string]interface{}, int64]
)

type User struct {
	ID         int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	UserID     int64     `gorm:"index;not null;comment:user_id" json:"user_id"`
	Nickname   string    `gorm:"type:varchar(32);not null;default:'';comment:昵称" json:"nickname"`
	HeadImgUrl string    `gorm:"type:varchar(255);not null;default:'';comment:头像地址" json:"head_img_url"`
	Sex        int32     `gorm:"not null;default:0;comment:性别（1男；2女）" json:"sex"`
	Phone      string    `gorm:"type:varchar(32);not null;default:'';comment:手机" json:"phone"`
	Email      string    `gorm:"type:varchar(125);not null;default:'';comment:邮件" json:"email"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (User) TableName() string {
	return "user"
}

func TestGetList(t *testing.T) {
	param := make(datapipe.Params, 0)
	param.WithPage(2, 5)
	list := source.GetList(param)
	j, _ := json.MarshalIndent(list, "", "   ")
	t.Log("\n", string(j))
}

func TestGetListTable(t *testing.T) {
	param := make(datapipe.Params, 0)
	param.WithPage(1, 5)
	param.WithScopes(func(db *gorm.DB) *gorm.DB {
		db.Order("id DESC")
		return db.Select([]string{
			"id", "user_id", "nickname", "created_at",
		})
	})

	list := sourceTable.GetList(param)
	t.Log("\n", fmtJson(list))
}

func TestGetItem(t *testing.T) {
	item, e := source.GetItem(1)
	if e != nil {
		t.Fatal(e)
	}
	t.Log("\n", fmtJson(item))
}

func TestStore(t *testing.T) {
	var user = User{
		Nickname:   "张三",
		HeadImgUrl: "dddddddd.jpg",
		Sex:        0,
		Phone:      "13654654512",
		Email:      "22@qq.com",
	}
	source.Store(&user)
	t.Log(fmtJson(user))
}

func TestUpdate(t *testing.T) {
	var user = User{
		ID:         1,
		Nickname:   "张三",
		HeadImgUrl: "dddddddd.jpg",
		Sex:        0,
		Phone:      "13654654512",
		Email:      "22@qq.com",
	}
	source.Update(1, &user)
	t.Log(fmtJson(user))
}

func fmtJson(v interface{}) string {
	j, _ := json.MarshalIndent(v, "", "   ")

	return string(j)
}

func init() {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer（日志输出的目标，前缀和日志包含的内容——译者注）
		logger.Config{
			SlowThreshold:             time.Second, // 慢 SQL 阈值
			LogLevel:                  logger.Info, // 日志级别
			IgnoreRecordNotFoundError: true,        // 忽略ErrRecordNotFound（记录未找到）错误
		},
	)

	db, e := gorm.Open(mysql.Open(DSN), &gorm.Config{
		Logger: newLogger,
	})
	if e != nil {
		log.Fatal(e)
	}
	// db.Migrator().AutoMigrate(&User{})
	source = datapipe.NewDBDataSource[User, int64](db)
	sourceTable = datapipe.NewDBDataTableSource[map[string]interface{}, int64](db, "user")
}
