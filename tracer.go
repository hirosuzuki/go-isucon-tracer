package tracer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	proxy "github.com/shogo82148/go-sql-proxy"
)

// SQLLoggerInit ... Register SQL Logger Drivers
func SQLLoggerInit(logFileName string) {

	logger, err := os.Create(logFileName)
	if err != nil {
		log.Fatal(err)
	}

	rep := regexp.MustCompile(`[ \n\t]{2,}`)

	PreFunc := func(c context.Context, stmt *proxy.Stmt, args []driver.NamedValue) (interface{}, error) {
		return time.Now().UnixNano(), nil
	}
	PostFunc := func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, err error) error {
		now := time.Now()
		startTime := ctx.(int64)
		timeDelta := now.UnixNano() - startTime
		query := rep.ReplaceAllString(stmt.QueryString, " ")
		record := fmt.Sprintf("%d\t%d\t%s", startTime, timeDelta, query)
		fmt.Fprintln(logger, record)
		return nil
	}

	for _, driverName := range sql.Drivers() {
		if strings.Contains(driverName, ":logger") {
			continue
		}
		db, _ := sql.Open(driverName, "")
		defer db.Close()
		newDriverName := driverName + ":logger"
		log.Printf("Register: %s\n", newDriverName)
		sql.Register(driverName+":logger", proxy.NewProxyContext(db.Driver(), &proxy.HooksContext{
			PreExec: PreFunc,
			PostExec: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, result driver.Result, err error) error {
				return PostFunc(c, ctx, stmt, args, err)
			},
			PreQuery: PreFunc,
			PostQuery: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, rows driver.Rows, err error) error {
				return PostFunc(c, ctx, stmt, args, err)
			},
		}))
	}
}
