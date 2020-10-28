package com.inforefiner.rtcflow;

import io.github.classgraph.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main2 {

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            try (ScanResult scanResult = new ClassGraph()
                    .enableAllInfo()
                    .scan()) {

                ClassInfoList routeClassInfoList = scanResult.getClassesWithAnnotation("com.merce.woven.annotation.StepBind");
                for (ClassInfo routeClassInfo : routeClassInfoList) {
                    // Get the Route annotation on the class
                    String classInfoName = routeClassInfo.getName();
                    System.out.println("class name is "  + classInfoName);
                    AnnotationInfo annotationInfo = routeClassInfo.getAnnotationInfo("com.merce.woven.annotation.StepBind");
                    AnnotationParameterValueList parameterValues = annotationInfo.getParameterValues();
                    int size = parameterValues.size();
                    for (int i = 0; i < size; i++) {
                        AnnotationParameterValue parameterValue = parameterValues.get(i);
                        String name = parameterValue.getName();
                        Object value = parameterValue.getValue();
                        System.out.println(name + " : " + value.toString());
                    }
                }
            }
            System.out.println("-------------------------------------------------");
            Thread.sleep(100);
        }

    }

}
