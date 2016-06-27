package com.twitter.heron.scheduler.kubernetes;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.SchedulerUtils;

import java.util.logging.Logger;

public class KubernetesLauncher implements ILauncher {
    private static final Logger LOG = Logger.getLogger(KubernetesLauncher.class.getName());

    Config config;
    Config runtime;

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
    public boolean launch(PackingPlan packing) {
        return SchedulerUtils.onScheduleAsLibrary(config, runtime, getScheduler(), packing);
    }

    protected IScheduler getScheduler() {
        return new KubernetesScheduler();
    }
}
