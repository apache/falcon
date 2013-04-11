package org.apache.falcon.resource.channel;

import java.util.Arrays;

public class MethodKey {

    private final String name;
    private final Class[] argClasses;

    public MethodKey(String name, Object[] args) {
        this.name = name;
        argClasses = new Class[args.length];
        for (int index = 0; index < args.length; index++) {
            if (args[index] != null) {
                argClasses[index] = args[index].getClass();
            }
        }
    }

    public MethodKey(String name, Class[] args) {
        this.name = name;
        argClasses = args;
    }

    @Override
    public boolean equals(Object methodRHS) {
        if (this == methodRHS) return true;
        if (methodRHS == null ||
                getClass() != methodRHS.getClass()) return false;

        MethodKey methodKey = (MethodKey) methodRHS;

        if (name != null ? !name.equals(methodKey.name) :
                methodKey.name != null) return false;
        boolean matching = true;
        for (int index = 0; index < argClasses.length; index++) {
            if (argClasses[index] != null && methodKey.argClasses[index] != null &&
                    !methodKey.argClasses[index].isAssignableFrom(argClasses[index])) {
                 matching = false;
            }
        }
        return matching;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (argClasses.length);
        return result;
    }

    @Override
    public String toString() {
        return "MethodKey{" +
                "name='" + name + '\'' +
                ", argClasses=" + (argClasses == null ? null : Arrays.asList(argClasses)) +
                '}';
    }
}
