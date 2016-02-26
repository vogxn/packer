package ebs

import (
	"database/sql"
	"fmt"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/mattn/go-sqlite3"
	"github.com/mitchellh/multistep"
	awscommon "github.com/mitchellh/packer/builder/amazon/common"
	"github.com/mitchellh/packer/packer"
)

type stepCreateAMI struct {
	image *ec2.Image
}

func (s *stepCreateAMI) Run(state multistep.StateBag) multistep.StepAction {
	config := state.Get("config").(Config)
	ec2conn := state.Get("ec2").(*ec2.EC2)
	instance := state.Get("instance").(*ec2.Instance)
	image := state.Get("source_image").(*ec2.Image)
	ui := state.Get("ui").(packer.Ui)

	// Create the image
	ui.Say(fmt.Sprintf("Creating the AMI: %s", config.AMIName))
	createOpts := &ec2.CreateImageInput{
		InstanceId:          instance.InstanceId,
		Name:                &config.AMIName,
		BlockDeviceMappings: config.BlockDevices.BuildAMIDevices(),
	}

	createResp, err := ec2conn.CreateImage(createOpts)
	if err != nil {
		err := fmt.Errorf("Error creating AMI: %s", err)
		state.Put("error", err)
		ui.Error(err.Error())
		return multistep.ActionHalt
	}

	// Set the AMI ID in the state
	ui.Message(fmt.Sprintf("AMI: %s", *createResp.ImageId))
	amis := make(map[string]string)
	amis[*ec2conn.Config.Region] = *createResp.ImageId
	state.Put("amis", amis)

	// Wait for the image to become ready
	stateChange := awscommon.StateChangeConf{
		Pending:   []string{"pending"},
		Target:    "available",
		Refresh:   awscommon.AMIStateRefreshFunc(ec2conn, *createResp.ImageId),
		StepState: state,
	}

	ui.Say("Waiting for AMI to become ready...")
	if _, err := awscommon.WaitForState(&stateChange); err != nil {
		err := fmt.Errorf("Error waiting for AMI: %s", err)
		state.Put("error", err)
		ui.Error(err.Error())
		return multistep.ActionHalt
	}

	// In the following block of code we write the AMI id generated for the region into a
	// local sqlite db. This allows us to retry AMI baking only for regions which do not have
	// a valid AMI entry in the db
	var ami_type string
	if "paravirtual" == *image.VirtualizationType {
		ami_type = "pv"
	} else {
		ami_type = "hvm"
	}

	ui.Say("AMI created. Updating the database now.")
	var DB_DRIVER string
	sql.Register(DB_DRIVER, &sqlite3.SQLiteDriver{})
	db, err := sql.Open(DB_DRIVER, "pacman.db")
	checkErr(err, state, "failed to create the database handle")

	stmt, err := db.Prepare("update bake_ami set ami_status=1, ami_id=? where region=? and ami_type=?")
	checkErr(err, state, "preparing update query failed")

	res, err := stmt.Exec(*createResp.ImageId, *ec2conn.Config.Region, ami_type)
	checkErr(err, state, "update execution failed")

	affect, err := res.RowsAffected()
	checkErr(err, state, "update db failed")

	ui.Say(fmt.Sprintf("Updated database with %d row(s) affected", affect))
	db.Close()

	imagesResp, err := ec2conn.DescribeImages(&ec2.DescribeImagesInput{ImageIds: []*string{createResp.ImageId}})
	if err != nil {
		err := fmt.Errorf("Error searching for AMI: %s", err)
		state.Put("error", err)
		ui.Error(err.Error())
		return multistep.ActionHalt
	}
	s.image = imagesResp.Images[0]

	return multistep.ActionContinue
}

func checkErr(err error, state multistep.StateBag, msg string) {
	if err != nil {
		err := fmt.Errorf(msg)
		state.Put("error", err)
		ui := state.Get("ui").(packer.Ui)
		ui.Error(err.Error())
	}
}

func (s *stepCreateAMI) Cleanup(state multistep.StateBag) {
	if s.image == nil {
		return
	}

	_, cancelled := state.GetOk(multistep.StateCancelled)
	_, halted := state.GetOk(multistep.StateHalted)
	if !cancelled && !halted {
		return
	}

	ec2conn := state.Get("ec2").(*ec2.EC2)
	ui := state.Get("ui").(packer.Ui)

	ui.Say("Deregistering the AMI because cancelation or error...")
	deregisterOpts := &ec2.DeregisterImageInput{ImageId: s.image.ImageId}
	if _, err := ec2conn.DeregisterImage(deregisterOpts); err != nil {
		ui.Error(fmt.Sprintf("Error deregistering AMI, may still be around: %s", err))
		return
	}
}
