package src

// Pixels serves CPA for publishers
// If someone has paid, we notify publisher with the pixel in url
// We pay for each request.
// It gives us more targeted traffic from publisher
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/linkit360/go-pixel/src/config"
	"github.com/linkit360/go-pixel/src/service"
	m "github.com/linkit360/go-utils/metrics"
)

func RunServer() {
	appConfig := config.LoadConfig()

	service.InitService(
		appConfig.AppName,
		appConfig.Service,
		appConfig.Server,
		appConfig.MidConfig,
		appConfig.DbConf,
		appConfig.Consumer,
		appConfig.Notifier,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()

	service.AddPixelsHandler(r)
	service.AddPublisherHandler(r)
	m.AddHandler(r)

	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("pixels init")
}
