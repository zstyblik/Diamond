# coding=utf-8

"""
Collect pre-scraped data from S.M.A.R.T. Based on smart collector.

#### Dependencies

 * [smartmontools](http://sourceforge.net/apps/trac/smartmontools/wiki)
"""

import diamond.collector
import re
import os

class SmartPassiveCollector(diamond.collector.Collector):
    """ SMART Collector for Diamond """

    def get_default_config_help(self):
        config_help = super(SmartPassiveCollector, self).get_default_config_help()
        config_help.update({
            "data_path": "Path to outputs from smartctl",
            "out_file_regex": "Regext to match output files from smartctl"
        })
        return config_help

    def get_default_config(self):
        """
        Returns default configuration options.
        """
        config = super(SmartPassiveCollector, self).get_default_config()
        config.update({
            "path": "smart",
            "data_path": "/var/lib/smartmontools/nagios/",
            "out_file_regex": ".+\.out$",
        })
        return config

    def collect(self):
        """
        Collect and publish S.M.A.R.T. attributes
        """
        if not os.path.isdir(self.config["data_path"]):
            self.log.error("Directory '%s' doesn't exist.",
                    self.config["data_path"])
            return {}

        re_outfile = re.compile(self.config["out_file_regex"])
        re_ata_attr = re.compile("\s*(\d+)\s(\S+)\s+(?:\S+\s+){6}(\S+)\s+(\d+)")
        for dir_item in os.listdir(self.config["data_path"]):
            self.log.debug("Got '%s'.", dir_item)
            if not re_outfile.search(dir_item):
                self.log.debug("It didn't match regex.")
                continue

            device = re.sub(r'\..+', '', dir_item)
            fpath = os.path.join(self.config["data_path"], dir_item)
            self.log.debug("Will read '%s'.", fpath)
            with open(fpath, "r") as fhandle:
                lines = fhandle.readlines()

            metrics = {}

            for line in lines:
                match = re_ata_attr.search(line)
                if not match:
                    continue

                metric = "%s.%s" % (device, match.group(2).lower())
                if metric not in metrics:
                    metrics[metric] = match.group(4)
                elif metrics[metric] == 0 and int(match.group(4)) > 0:
                    metrics[metric] = match.group(4)
                else:
                    continue

            for metric in metrics.keys():
                self.publish(metric, metrics[metric])
