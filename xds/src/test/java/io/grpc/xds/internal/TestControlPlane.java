package io.grpc.xds.internal;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.TlsServerCredentials;
import io.grpc.alts.AltsServerCredentials;
import io.grpc.internal.testing.TestUtils;
import io.grpc.xds.XdsTestControlPlaneExternalService;
import io.grpc.xds.XdsTestControlPlaneExternalWrapperService;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestControlPlane {

  private XdsTestControlPlaneExternalService controlPlaneService =
      new XdsTestControlPlaneExternalService();
  private XdsTestControlPlaneExternalWrapperService wrapperService =
      new XdsTestControlPlaneExternalWrapperService(controlPlaneService);
  private Server server;
  private int port = 8070;
  private boolean useTls = false;
  private boolean useAlts = false;

  /**
   * The main application allowing this client to be launched from the command line.
   */
  public static void main(String[] args) throws Exception {
    final TestControlPlane server = new TestControlPlane();
    server.parseArgs(args);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              @SuppressWarnings("CatchAndPrintStackTrace")
              public void run() {
                try {
                  System.out.println("Shutting down");
                  server.stop();
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            });
    server.start();
    System.out.println("Server started on port " + server.port);
    server.blockUntilShutdown();
  }


  @VisibleForTesting
  void parseArgs(String[] args) {
    boolean usage = false;
    for (String arg : args) {
      String[] parts = arg.substring(2).split("=", 2);
      String key = parts[0];
      if (!isArgValid(arg, key, parts)) {
        usage = true;
        break;
      }
      String value = parts[1];
      if ("port".equals(key)) {
        port = Integer.parseInt(value);
      } else if ("use_tls".equals(key)) {
        useTls = Boolean.parseBoolean(value);
      } else if ("use_alts".equals(key)) {
        useAlts = Boolean.parseBoolean(value);
      } else if ("grpc_version".equals(key)) {
        if (!"2".equals(value)) {
          System.err.println("Only grpc version 2 is supported");
          usage = true;
          break;
        }
      } else {
        System.err.println("Unknown argument: " + key);
        usage = true;
        break;
      }
    }
    if (useAlts) {
      useTls = false;
    }
    if (usage) {
      TestControlPlane s = new TestControlPlane();
      System.out.println(
          "Usage: [ARGS...]"
              + "\n"
              + "\n  --port=PORT           Port to connect to. Default " + s.port
              + "\n  --use_tls=true|false  Whether to use TLS. Default " + s.useTls
              + "\n  --use_alts=true|false Whether to use ALTS. Enable ALTS will disable TLS."
              + "\n                        Default " + s.useAlts
      );
      System.exit(1);
    }
  }

  private boolean isArgValid(String arg, String key, String[] parts) {
    if (!arg.startsWith("--")) {
      System.err.println("All arguments must start with '--': " + arg);
      return true;
    }
    if ("help".equals(key)) {
      return true;
    }
    if (parts.length != 2) {
      System.err.println("All arguments must be of the form --arg=value");
      return true;
    }
    return false;
  }

  @VisibleForTesting
  void start() throws Exception {
    ServerCredentials serverCreds;
    if (useAlts) {
        serverCreds = AltsServerCredentials.create();
    } else if (useTls) {
      serverCreds = TlsServerCredentials.create(
          TestUtils.loadCert("server1.pem"), TestUtils.loadCert("server1.key"));
    } else {
      serverCreds = InsecureServerCredentials.create();
    }

    server = Grpc.newServerBuilderForPort(port, serverCreds)
        .addService(controlPlaneService)
        .addService(wrapperService)
        .build()
        .start();
  }

  @VisibleForTesting
  void stop() throws Exception {
    server.shutdownNow();
    if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
      System.err.println("Timed out waiting for server shutdown");
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

}
