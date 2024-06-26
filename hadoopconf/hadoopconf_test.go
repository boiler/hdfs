package hadoopconf

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfFallback(t *testing.T) {
	oldHome := os.Getenv("HADOOP_HOME")
	oldConfDir := os.Getenv("HADOOP_CONF_DIR")
	os.Setenv("HADOOP_HOME", "testdata") // This will resolve to testdata/conf.
	os.Setenv("HADOOP_CONF_DIR", "testdata/conf2")

	confNamenodes := []string{"namenode1:8020", "namenode2:8020"}
	conf2Namenodes := []string{"namenode3:8020"}

	conf, err := LoadFromEnvironment()
	assert.NoError(t, err)

	nns := conf.Namenodes()
	assert.NoError(t, err)
	assert.EqualValues(t, conf2Namenodes, nns, "loading via HADOOP_CONF_DIR (testdata/conf2)")

	os.Unsetenv("HADOOP_CONF_DIR")

	conf, err = LoadFromEnvironment()
	assert.NoError(t, err)

	nns = conf.Namenodes()
	assert.NoError(t, err)
	assert.EqualValues(t, confNamenodes, nns, "loading via HADOOP_HOME (testdata/conf)")

	os.Setenv("HADOOP_HOME", oldHome)
	os.Setenv("HADOOP_CONF_DIR", oldConfDir)

}

func TestConfNameservices(t *testing.T) {
	os.Setenv("HADOOP_CONF_DIR", "testdata/conf3")

	conf, err := LoadFromEnvironment()
	assert.NoError(t, err)

	nsMap := conf.Nameservices()
	assert.NoError(t, err)

	assert.NotContains(t, nsMap, "anothercluster")

	assert.EqualValues(
		t,
		[]string{"defaulthost"},
		nsMap[""].Namenodes,
		"default nameservice",
	)

	assert.EqualValues(
		t,
		[]string{"namenode3:8020", "namenode1:8020"},
		nsMap["mycluster"].Namenodes,
		"mycluster namenodes",
	)
}
