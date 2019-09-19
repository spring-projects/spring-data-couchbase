package org.springframework.data.couchbase;

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.WaitStrategy;

/**
 * WaitStrategy for Couchbase containers which makes the Server node is
 * initialized, RBAC user and default bucket is created.
 */
public class CouchbaseWaitStrategy implements WaitStrategy {

    private Duration startupTimeout = Duration.of(60, SECONDS);

    public CouchbaseWaitStrategy() {

    }

    private void checkResult(Container.ExecResult result, String command) throws Exception {
        if (!result.getStdout().contains("SUCCESS")) {
            throw new Exception(command + " command failed");
        }
    }

    private void checkService(int port, String path) {
        Callable<Boolean> externalCheck = new CouchbaseHttpPortListeningCheck(port, path);
        Unreliables.retryUntilSuccess((int) startupTimeout.getSeconds(), TimeUnit.SECONDS, () ->
                 externalCheck.call());
    }

    @Override
    public void waitUntilReady(GenericContainer container) {
        try {
            checkService(8091, "/pools");
            Container.ExecResult result;

            result = container.execInContainer("/opt/couchbase/bin/couchbase-cli",
                    "cluster-init",
                    "--cluster=127.0.0.1:8091",
                    "--services=data,index,query",
                    "--cluster-name=localcontainer",
                    "--cluster-username=Administrator",
                    "--cluster-password=password",
                    "--cluster-ramsize=512",
                    "--cluster-index-ramsize=512",
                    "--index-storage-setting=default");
            checkResult(result, "Cluster init");
            result = container.execInContainer("/opt/couchbase/bin/couchbase-cli",
                    "user-manage",
                    "--cluster=127.0.0.1:8091",
                    "--username=Administrator",
                    "--password=password",
                    "--set",
                    "--rbac-username=protected",
                    "--rbac-password=password",
                    "--rbac-name=default",
                    "--roles=admin",
                    "--auth-domain=local");
            checkResult(result, "User manage");
            result = container.execInContainer("/opt/couchbase/bin/couchbase-cli",
                    "bucket-create",
                    "--cluster=127.0.0.1:8091",
                    "--username=Administrator",
                    "--password=password",
                    "--bucket=protected",
                    "--bucket-type=couchbase",
                    "--bucket-ramsize=200",
                    "--enable-flush=1",
                    "--wait");
            checkResult(result, "Bucket create");
            checkService(8093, "/query/ping");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
        this.startupTimeout = startupTimeout;
        return this;
    }
}