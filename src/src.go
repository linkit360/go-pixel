package src

// Pixels serves CPA for publishers
// If someone has paid, we notify publisher with the pixel in url
// We pay for each request.
// It gives us more targeted traffic from publisher
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	m "github.com/vostrok/metrics"
	"github.com/vostrok/pixels/src/config"
	"github.com/vostrok/pixels/src/service"
)

func RunServer() {
	appConfig := config.LoadConfig()
	m.Init(appConfig.Name)

	service.InitService(
		appConfig.Service,
		appConfig.Server,
		appConfig.DbConf,
		appConfig.Consumer,
		appConfig.Notifier,
	)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()

	service.AddPixelsHandler(r)
	service.AddCQRHandler(r)
	service.AddPublisherHandler(r)
	m.AddHandler(r)

	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("pixels init")
}
