package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")

var ERR_WRONG_VERSION = fmt.Errorf("Update Rejected because of wrong version")
