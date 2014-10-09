package org.apache.falcon.regression.core.supportClasses;

public final class ExecResult {

    private final int exitVal;
    private final String output;
    private final String error;

    public ExecResult(final int exitVal, final String output, final String error) {
        this.exitVal = exitVal;
        this.output = output;
        this.error = error;
    }

    public int getExitVal() {
        return exitVal;
    }

    public String getOutput() {
        return output;
    }

    public String getError() {
        return error;
    }
}
