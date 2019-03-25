package com.sf.misc.hadoop.recover;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import javax.swing.*;


public class WriteEditLogOP {

    protected final FSEditLogOpCodes type;
    protected final long txid;

    protected final long timestamp;
    protected final Path from;
    protected final Path to;

    public WriteEditLogOP(FSEditLogOpCodes type, long txid, long timestamp, Path from, Path to) {
        this.type = type;
        this.txid = txid;
        this.timestamp = timestamp;
        this.from = from;
        this.to = to;
    }

    public WriteEditLogOP(FSEditLogOp op) {
        this.type = op.opCode;
        this.txid = op.getTransactionId();

        switch (this.type) {
            case OP_RENAME_OLD:
            case OP_RENAME:
                this.timestamp = FSEditLogOpSniper.get(op, "timestamp");
                this.from = new Path(FSEditLogOpSniper.get(op, "src").toString());
                this.to = new Path(FSEditLogOpSniper.get(op, "dst").toString());
                break;
            case OP_DELETE:
                this.timestamp = FSEditLogOpSniper.get(op, "timestamp");
                this.to = this.from = new Path(FSEditLogOpSniper.get(op, "path").toString());
                break;
            default:
                throw new UnsupportedOperationException("not supoorted op:" + op);
        }
    }

    public static boolean accept(FSEditLogOp op) {
        switch (op.opCode) {
            case OP_RENAME_OLD:
            case OP_RENAME:
            case OP_DELETE:
                return true;
            default:
                return false;
        }
    }

    public FSEditLogOpCodes type() {
        return type;
    }

    public long txid() {
        return txid;
    }

    public long timestamp() {
        return timestamp;
    }

    public Path from() {
        return from;
    }

    public Path to() {
        return to;
    }

    @Override
    public String toString() {
        return "type:" + type() + " txid:" + txid() + " from:" + from() + " to:" + to();
    }
}
