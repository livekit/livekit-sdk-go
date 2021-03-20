// +build mage

package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/magefile/mage/target"
)

// regenerate protobuf
func Proto() error {
	protoDir := "../protocol"
	updated, err := target.Path("proto/livekit_models.pb.go",
		protoDir+"/livekit_models.proto",
		protoDir+"/livekit_room.proto",
		protoDir+"/livekit_rtc.proto",
	)
	if err != nil {
		return err
	}
	if !updated {
		return nil
	}

	fmt.Println("generating protobuf")
	target := "proto/"
	if err := os.MkdirAll(target, 0755); err != nil {
		return err
	}

	// generate model and room
	cmd := exec.Command("protoc",
		"--go_out", target,
		"--twirp_out", target,
		"--go_opt=paths=source_relative",
		"--twirp_opt=paths=source_relative",
		"-I="+protoDir,
		protoDir+"/livekit_room.proto",
	)
	connectStd(cmd)
	if err := cmd.Run(); err != nil {
		return err
	}

	// generate rtc
	cmd = exec.Command("protoc",
		"--go_out", target,
		"--go_opt=paths=source_relative",
		"-I="+protoDir,
		protoDir+"/livekit_rtc.proto",
		protoDir+"/livekit_models.proto",
	)
	connectStd(cmd)
	if err := cmd.Run(); err != nil {
		return err
	}

	return nil
}

func connectStd(cmd *exec.Cmd) {
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
}
