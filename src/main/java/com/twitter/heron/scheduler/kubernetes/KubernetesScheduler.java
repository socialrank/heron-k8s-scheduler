package com.twitter.heron.scheduler.kubernetes;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.scheduler.IScheduler;

import java.io.File;
import java.util.List;
import java.util.logging.Logger;

public class KubernetesScheduler implements IScheduler {
    private static final Logger LOG = Logger.getLogger(KubernetesScheduler.class.getName());

    Config config;
    Config runtime;

    String kubectlPath;
    @Override
    public void initialize(Config mConfig, Config mRuntime) {
        this.config = mConfig;
        this.runtime = mRuntime;
    }

    @Override
    public void close() {
        //NOP
    }

    @Override
    public boolean onSchedule(PackingPlan packing) {
        if (packing == null || packing.containers.isEmpty()) {
            LOG.severe("No container (worker) requested.  Can't schedule, aborting.");
            return false;
        }

        LOG.info("Launching topology in Kubernetes");


        kubectlPath = config.getStringValue("heron.scheduler.kubernetes.kubectl.path");

        StringBuilder stdout = new StringBuilder();
        ShellUtils.runProcess(true, "kubectl get nodes", stdout, new StringBuilder());

        LOG.info(stdout.toString());
        return false;
    }

    @Override
    public List<String> getJobLinks() {
        return null;
    }

    @Override
    public boolean onKill(com.twitter.heron.proto.scheduler.Scheduler.KillTopologyRequest killTopologyRequest) {
        return false;
    }

    @Override
    public boolean onRestart(com.twitter.heron.proto.scheduler.Scheduler.RestartTopologyRequest restartTopologyRequest) {
        return false;
    }

    // Kubernetes related stuff

    protected String getKubernetesReplicationControllerPath() {
        return new File(Context.heronConf(config), "kubernetes-rc.json").getPath();
    }
}