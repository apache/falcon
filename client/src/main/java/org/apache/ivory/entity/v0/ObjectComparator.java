package org.apache.ivory.entity.v0;

import org.apache.commons.codec.digest.DigestUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObjectComparator {

    private static Map<Class, List<Method>> methods =
            new HashMap<Class, List<Method>>();

    private static final Package JAVA_LANG = String.class.getPackage();
    private static final String GET_CLASS = "getClass";
    private static final String GETTER = "get";

    private static final char DELIM = '\u0001';
    private static final char LIST_SEP = '\u0003';
    private static final char NULL = '\u0002';
    private static final String IVORY_PACKAGE = "org.apache.ivory";

    public static boolean equals(Entity lhs, Entity rhs) {
        if (lhs == null && rhs == null) return true;
        if (lhs == null || rhs == null) return false;

        if (lhs.equals(rhs)) {
            String lhsString = stringOf(lhs);
            String rhsString = stringOf(rhs);
            return lhsString.equals(rhsString);
        } else {
            return false;
        }
    }

    public static byte[] md5(Entity entity) {
        return DigestUtils.md5(stringOf(entity));
    }

    private static String stringOf(Object object)  {

        try {
            Class clazz = object.getClass();
            if (!clazz.getName().startsWith(IVORY_PACKAGE)) {
                return String.valueOf(object);
            }

            if (!methods.containsKey(clazz)) {
                populateGetters(clazz);
            }
            StringBuilder buffer = new StringBuilder();

            for (Method method : methods.get(clazz)) {
                Object result = method.invoke(object);
                if (result == null) {
                    buffer.append(NULL).append(DELIM);
                } else if (Enum.class.isAssignableFrom(result.getClass())) {
                    buffer.append(((Enum)result).name()).append(DELIM);
                } else if (result.getClass().getPackage().equals(JAVA_LANG)) {
                    buffer.append(String.valueOf(result)).append(DELIM);
                } else if (List.class.isAssignableFrom(result.getClass())) {
                    for (Object element : (List) result) {
                        buffer.append(stringOf(element)).append(LIST_SEP);
                    }
                } else {
                    buffer.append(stringOf(result)).append(DELIM);
                }
            }
            return buffer.toString();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to invoke getter", e);
        }
    }

    private synchronized static void populateGetters(Class clazz) {
        if (methods.containsKey(clazz)) return;

        List<Method> getters = new ArrayList<Method>();
        for (Method method : clazz.getMethods()) {
            if (method.getParameterTypes().length == 0 &&
                    method.getName().startsWith(GETTER) &&
                    ! method.getName().equals(GET_CLASS) ) {
                getters.add(method);
            }
        }
        methods.put(clazz, getters);
    }
}
