package com.sf.misc.hadoop.recover;

import com.google.gson.Gson;
import com.sun.corba.se.pept.protocol.ProtocolHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cors.CorsHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;
import org.apache.http.HttpStatus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TailingService {

    public static final Log LOGGER = LogFactory.getLog(TailingService.class);

    protected static Promise.PromiseConsumer<EditLogTailer.FSOpStat> startHTTPServer(int port) {
        AtomicReference<EditLogTailer.FSOpStat> holder = new AtomicReference<>();

        Promise.PromiseSupplier<Optional<EditLogTailer.FSOpStat>> supplier = () -> Optional.ofNullable(holder.get());

        new ServerBootstrap().group(new NioEventLoopGroup())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_BACKLOG, 30000)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new SimpleChannelInboundHandler<HttpObject>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
                                        if (msg instanceof HttpRequest) {
                                            ByteBuf buf = ctx.alloc().buffer();
                                            byte[] serialized = supplier.get().map((value) -> new Gson().toJson(value))
                                                    .orElse("{}")
                                                    .getBytes();
                                            buf.writeBytes(serialized);
                                            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buf));
                                            return;
                                        }
                                    }
                                })
                        ;
                    }
                })
                .bind(port);

        return holder::set;
    }

    public static void main(String[] args) {
        // load config
        Properties properties = new Properties();
        URL config = Thread.currentThread().getContextClassLoader().getResource("config.property");
        if (config == null) {
            throw new UncheckedIOException(new FileNotFoundException("no config.property found"));
        }

        try (InputStream input = config.openStream()) {
            properties.load(input);
        } catch (IOException e) {
            throw new UncheckedIOException("fail to open config:" + config, e);
        }

        // print
        properties.forEach((key, value) -> {
            LOGGER.info("using config:" + key + " = " + value);
        });

        Promise.PromiseConsumer<EditLogTailer.FSOpStat> http_stat_listener = startHTTPServer(Integer.valueOf(properties.getProperty("http_port", "100888")));

        new EditLogTailer(
                new EditLogArchive(new File(properties.getOrDefault("storage", "__storage__").toString())),
                new LogAggregator(new NamenodeRPC(
                        URI.create(properties.get("nameservice").toString()),
                        properties.getProperty("runas", "hdfs")
                ), true),
                (op) -> {
                    // reject not rename op
                    if (op.opCode.compareTo(FSEditLogOpCodes.OP_RENAME_OLD) != 0) {
                        return false;
                    }

                    // skip if not in trash
                    if (!RenameOldOpSerializer.target(op).contains(".Trash")) {
                        return false;
                    }
                    return true;
                },
                RenameOldOpSerializer::lineSerialize,
                (stat) -> {
                    LOGGER.info("stat:" + stat);
                    http_stat_listener.accept(stat);
                }
        ).start(Long.valueOf(properties.getOrDefault("poll_period", "" + TimeUnit.MINUTES.toMillis(1)).toString()),
                Long.valueOf(properties.getOrDefault("expiration_for_log", "" + TimeUnit.DAYS.toMillis(365)).toString())
        ).logException().join();
    }
}
