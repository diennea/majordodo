/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package majordodo.utils;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utils for configuration
 *
 * @author enrico.olivelli
 */
public class ReflectionUtils {

    private static final Logger LOGGER = Logger.getLogger(ReflectionUtils.class.getName());

    public static void apply(Map<String, Object> properties, Object object) {
        LOGGER.log(Level.FINEST, "Applying " + properties + " to " + object);
        Map<String, Object> notCaseSensitive = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        notCaseSensitive.putAll(properties);
        try {
            BeanInfo info = Introspector.getBeanInfo(object.getClass());
            for (PropertyDescriptor desc : info.getPropertyDescriptors()) {
                if (desc.getWriteMethod() != null) {
                    Object value = notCaseSensitive.get(desc.getName());
                    if (value != null) {
                        Object coerced = corceValue(value, desc.getWriteMethod().getParameterTypes()[0]);
                        LOGGER.log(Level.CONFIG, "applying " + desc.getName() + "=" + coerced + " to " + object);
                        desc.getWriteMethod().invoke(object, new Object[]{coerced});
                    }
                }
            }
        } catch (IllegalAccessException | IllegalArgumentException | IntrospectionException | InvocationTargetException err) {
            throw new RuntimeException(err);
        }
    }

    private static Object corceValue(Object value, Class<?> parameterType) {
        if (value.getClass().isAssignableFrom(parameterType)) {
            return value;
        }
        // only interesting types for Majordodo configuration properties
        if (parameterType.equals(String.class
        )) {
            return value.toString();
        }

        if (parameterType.equals(Integer.class
        ) || parameterType.equals(Integer.TYPE)) {
            return Integer.parseInt(value.toString());
        }

        if (parameterType.equals(Long.class
        ) || parameterType.equals(Long.TYPE)) {
            return Long.parseLong(value.toString());
        }

        if (parameterType.equals(Boolean.class
        ) || parameterType.equals(Boolean.TYPE)) {
            return Boolean.parseBoolean(value.toString());
        }
        return value;
    }
}
