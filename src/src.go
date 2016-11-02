package src

// Pixels serves CPA for publishers
// If someone has paid, we notify publisher with the pixel in url
// We pay for each request.
// It gives us more targeted traffic from publisher
import (
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/contrib/expvar"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/pixels/src/config"
	"github.com/vostrok/pixels/src/service"
)

func RunServer() {
	appConfig := config.LoadConfig()
	service.InitService(appConfig.Service)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	r := gin.New()

	rg := r.Group("/debug")
	rg.GET("/vars", expvar.Handler())

	r.Run(":" + appConfig.Server.Port)

	log.WithField("port", appConfig.Server.Port).Info("pixels init")
}
