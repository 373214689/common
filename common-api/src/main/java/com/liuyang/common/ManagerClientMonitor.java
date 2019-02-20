package com.liuyang.common;

import com.liuyang.log.Logger;

import java.util.Timer;
import java.util.TimerTask;

// 客户端监控线程。主要是为了防止某些操作长时间阻塞导致程序不能正运行。
// 检查周期为 1 秒， 即每一秒会检测一次是否超时。
public final class ManagerClientMonitor {
    public final static Logger logger = Logger.getLogger(ManagerClientMonitor.class);

    public static ManagerClientMonitor monitoring(ManagerClient client, long millis) {
        ManagerClientMonitor monitor =  new ManagerClientMonitor(client, millis);
        monitor.start();
        return monitor;
    }

    private Timer                  timer;
    /** 记录超时时制 */
    private long                   timeout;
    private String                 name;
    private volatile ManagerClient client;
    private volatile boolean       running = false;

    private ManagerClientMonitor(ManagerClient client, long millis) {
        this.client  = client;
        this.timeout = millis;
        this.name    = client.getConf().toString();
        this.timer = new Timer("ManagerClientMonitorTimer - " + name);
    }

    private long getClientTime() {
        try {
            return client.getLastConnectionTime();
        } catch (Exception e) {
            return -1;
        }
    }

    private void closeClient() {
        try {
            if (client != null) client.close();
        } catch (Exception e) {
            // do nothing
        }
    }

    private void start() {
        running = true;
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                try {
                    // 如果 monitor 提前关闭, running 为 false，则不会执行超时判定。
                    // 此外还需要判断 client 是否为空。
                    if (running && client != null) {
                        long curr = scheduledExecutionTime();
                        long last = getClientTime();
                        long diff = curr - last;
                        logger.debug("Client(%s):%d, current: %d, diff: %d.", name, last, curr, diff);
                        if (diff >= timeout) {
                            logger.debug("Time out (" + diff + "), client(" + name + ") will be close.");
                            stop();
                        } else{
                            if (client == null) {
                                stop();
                            } else if (!client.isConnected()) {
                                logger.debug("Client(" + name + ") disconnect, monitor stop.");
                                stop();
                            }
                        }
                    } else {
                        // 如果没有运行，则停止计时器
                        stop();
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    logger.error("Monitor(" + name + ") is halted unexpectedly.");
                    stop();
                }
            }
        }, 0, 1000);
    }

    public final void stop() {
        closeClient();
        if (timer != null)
            timer.cancel();
        running = false;
        timer   = null;
        name    = null;
        client  = null;
    }
}
