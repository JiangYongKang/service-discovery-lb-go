package lb

import "github.com/highcumontoa/service-discovery-lb-go/model"

func modelErrKind(err error) model.ErrorKind { return model.ErrKind(err) }
