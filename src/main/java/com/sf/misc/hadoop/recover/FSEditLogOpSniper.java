package com.sf.misc.hadoop.recover;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOpCodes;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FSEditLogOpSniper {

    public static final Log LOGGER = LogFactory.getLog(FSEditLogOpSniper.class);

    protected static final long EXPIRE = TimeUnit.MINUTES.toMillis(5);

    protected static final LoadingCache<Class<? extends FSEditLogOp>, List<String>> OP_FIELD_CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(EXPIRE, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<Class<? extends FSEditLogOp>, List<String>>() {

                protected List<Field> collectField(Class<?> type) {
                    if (!FSEditLogOp.class.isAssignableFrom(type)) {
                        return Collections.emptyList();
                    }

                    return Stream.of(
                            Arrays.stream(type.getDeclaredFields()),
                            Arrays.stream(type.getFields()),
                            collectField(type.getSuperclass()).stream()
                    ).flatMap(Function.identity())
                            .filter((field) -> {
                                return !(Modifier.isStatic(field.getModifiers())
                                        || Modifier.isFinal(field.getModifiers())
                                        || field.isSynthetic()
                                )
                                        ;
                            })
                            .collect(Collectors.groupingBy(Field::getName))
                            .entrySet().parallelStream()
                            .map((entry) -> {
                                return entry.getValue().stream().min((left, right) -> {
                                    Class<?> left_type = left.getDeclaringClass();
                                    Class<?> right_type = right.getDeclaringClass();
                                    if (left_type.equals(right_type)) {
                                        return 0;
                                    } else if (left_type.isAssignableFrom(right_type)) {
                                        // left is super class of right,so favor right
                                        return 1;
                                    } else {
                                        return -1;
                                    }
                                }).get();
                            })
                            .collect(Collectors.toList());
                }

                @Override
                public List<String> load(Class<? extends FSEditLogOp> type) {
                    return collectField(type).stream().map(Field::getName).collect(Collectors.toList());
                }
            });

    protected static final Cache<String, MethodHandle> OP_FIELD_GETTER = CacheBuilder.newBuilder()
            .expireAfterAccess(EXPIRE, TimeUnit.MILLISECONDS)
            .build();

    protected static final Cache<String, MethodHandle> OP_FIELD_SETTER = CacheBuilder.newBuilder()
            .expireAfterAccess(EXPIRE, TimeUnit.MILLISECONDS)
            .build();

    protected static final LoadingCache<Class<? extends FSEditLogOp>, MethodHandle> OP_CONSTRUCTOR = CacheBuilder.newBuilder()
            .expireAfterAccess(EXPIRE, TimeUnit.MILLISECONDS)
            .build(new CacheLoader<Class<? extends FSEditLogOp>, MethodHandle>() {

                protected Constructor<?> serachConstructor(Class<?> type) {
                    if (!FSEditLogOp.class.isAssignableFrom(type)) {
                        return null;
                    }

                    try {
                        return Stream.concat(
                                Arrays.stream(type.getConstructors()),
                                Arrays.stream(type.getDeclaredConstructors())
                        ).min(Comparator.comparing(Constructor::getParameterCount))
                                .orElseThrow(() -> new NoSuchMethodException("no constructor found for:" + type));
                    } catch (Throwable e) {
                        throw new RuntimeException("no constructor found for:" + type, e);
                    }
                }

                @Override
                public MethodHandle load(Class<? extends FSEditLogOp> type) throws Exception {
                    Constructor<?> constructor = serachConstructor(type);
                    constructor.setAccessible(true);

                    MethodHandle handle = MethodHandles.publicLookup().unreflectConstructor(constructor);

                    Class[] parameters = constructor.getParameterTypes();
                    for (int i = 0; i < parameters.length; i++) {
                        // special case for some constructor check
                        if (parameters[i].equals(FSEditLogOpCodes.class)) {
                            handle = MethodHandles.insertArguments(handle, i, FSEditLogOpCodes.OP_START_LOG_SEGMENT);
                            continue;
                        }

                        handle = MethodHandles.insertArguments(handle, i, new Object[]{null});
                    }

                    return handle;
                }
            });

    public static FSEditLogOp copy(FSEditLogOp op) {
        if (op == null) {
            return null;
        }

        // create a template
        FSEditLogOp holder = create(op);

        // set fields
        fields(op.getClass()).forEach((field) -> {
            set(holder, field, get(op, field));
        });

        return holder;
    }


    public static <T> T get(FSEditLogOp op, String field) {
        String key = op.getClass().getName() + field;

        try {
            return (T) OP_FIELD_GETTER.get(key, () -> {
                Field hit = searchField(op.getClass(), field);
                hit.setAccessible(true);

                return MethodHandles.publicLookup().unreflectGetter(hit)
                        .asType(MethodType.methodType(hit.getType(), Object.class));
            }).invoke(op);
        } catch (Throwable throwable) {
            throw new RuntimeException("fail to get filed:" + field + " of:" + op, throwable);
        }
    }

    public static void set(FSEditLogOp op, String field, Object value) {
        String key = op.getClass().getName() + field;

        try {
            OP_FIELD_SETTER.get(key, () -> {
                Field hit = searchField(op.getClass(), field);
                hit.setAccessible(true);

                return MethodHandles.publicLookup().unreflectSetter(hit)
                        .asType(MethodType.methodType(void.class, Object.class, hit.getType()));
            }).invoke(op, value);
        } catch (Throwable throwable) {
            throw new RuntimeException("fail to get filed:" + field + " of:" + op, throwable);
        }
    }

    protected static List<String> fields(Class<? extends FSEditLogOp> type) {
        return OP_FIELD_CACHE.getUnchecked(type);
    }

    protected static FSEditLogOp create(FSEditLogOp template) {
        try {
            return (FSEditLogOp) OP_CONSTRUCTOR.getUnchecked(template.getClass()).invoke();
        } catch (Throwable throwable) {
            throw new RuntimeException("fail to clone template:" + template, throwable);
        }
    }

    protected static Field searchField(Class<?> type, String name) {
        return Stream.concat(
                Arrays.stream(type.getFields()),
                Arrays.stream(type.getDeclaredFields())
        ).filter((field) -> field.getName().equals(name))
                .findAny()
                .orElseGet(() -> {
                    Class<?> parent = type.getSuperclass();
                    if (parent == null) {
                        return null;
                    }

                    return searchField(parent, name);
                });
    }
}
