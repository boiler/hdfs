// Package hadoopconf provides utilities for reading and parsing Hadoop's xml
// configuration files.
package hadoopconf

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type Nameservice struct {
	Name      string
	Namenodes []string
}

type property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []property `xml:"property"`
}

var confFiles = []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml"}

// HadoopConf represents a map of all the key value configutation
// pairs found in a user's hadoop configuration files.
type HadoopConf map[string]string

// LoadFromEnvironment tries to locate the Hadoop configuration files based on
// the environment, and returns a HadoopConf object representing the parsed
// configuration. If the HADOOP_CONF_DIR environment variable is specified, it
// uses that, or if HADOOP_HOME is specified, it uses $HADOOP_HOME/conf.
//
// If no configuration can be found, it returns a nil map. If the configuration
// files exist but there was an error opening or parsing them, that is returned
// as well.
func LoadFromEnvironment() (HadoopConf, error) {
	hadoopConfDir := os.Getenv("HADOOP_CONF_DIR")
	if hadoopConfDir != "" {
		if conf, err := Load(hadoopConfDir); conf != nil || err != nil {
			return conf, err
		}
	}

	hadoopHome := os.Getenv("HADOOP_HOME")
	if hadoopHome != "" {
		if conf, err := Load(filepath.Join(hadoopHome, "conf")); conf != nil || err != nil {
			return conf, err
		}
	}

	return nil, nil
}

// Load returns a HadoopConf object representing configuration from the
// specified path. It will parse core-site.xml, hdfs-site.xml, and
// mapred-site.xml.
//
// If no configuration files could be found, Load returns a nil map. If the
// configuration files exist but there was an error opening or parsing them,
// that is returned as well.
func Load(path string) (HadoopConf, error) {
	var conf HadoopConf

	for _, file := range confFiles {
		pList := propertyList{}
		f, err := ioutil.ReadFile(filepath.Join(path, file))
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			return conf, err
		}

		err = xml.Unmarshal(f, &pList)
		if err != nil {
			return conf, fmt.Errorf("%s: %s", path, err)
		}

		if conf == nil {
			conf = make(HadoopConf)
		}

		for _, prop := range pList.Property {
			conf[prop.Name] = prop.Value
		}
	}

	return conf, nil
}

// default namenodes
func (conf HadoopConf) Namenodes() []string {
	nss := conf.Nameservices()
	return nss[""].Namenodes
}

// map of configured nameservices with their namenode addresses
func (conf HadoopConf) Nameservices() map[string]*Nameservice {
	defaultNameservice := ""
	nameservices := []string{}
	nsNamenodes := make(map[string][]string)
	nsNamenodeAddrs := make(map[string]map[string]string)

	for key, value := range conf {
		if strings.Contains(key, "fs.default") {
			nnUrl, _ := url.Parse(value)
			defaultNameservice = nnUrl.Host
		} else if key == "dfs.nameservices" {
			nameservices = strings.Split(value, ",")
		} else if strings.HasPrefix(key, "dfs.namenode.rpc-address.") {
			s := strings.Split(key[len("dfs.namenode.rpc-address."):], ".")
			ns := s[0]
			nn := s[1]
			if _, ok := nsNamenodeAddrs[ns]; !ok {
				nsNamenodeAddrs[ns] = make(map[string]string)
			}
			nsNamenodeAddrs[ns][nn] = value
		} else if strings.HasPrefix(key, "dfs.ha.namenodes.") {
			ns := key[len("dfs.ha.namenodes."):]
			nsNamenodes[ns] = strings.Split(value, ",")
		}
	}

	rsMap := make(map[string]*Nameservice)

	for _, ns := range nameservices {
		nnAddrs := []string{}
		for _, nn := range nsNamenodes[ns] {
			nnAddrs = append(nnAddrs, nsNamenodeAddrs[ns][nn])
		}
		rsMap[ns] = &Nameservice{
			Name:      ns,
			Namenodes: nnAddrs,
		}
	}

	if _, ok := rsMap[defaultNameservice]; ok {
		rsMap[""] = rsMap[defaultNameservice]
	} else {
		rsMap[""] = &Nameservice{
			Name:      defaultNameservice,
			Namenodes: []string{defaultNameservice},
		}
	}

	return rsMap
}
