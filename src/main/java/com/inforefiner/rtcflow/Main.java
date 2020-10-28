package com.inforefiner.rtcflow;

import com.merce.woven.annotation.StepBind;
import com.merce.woven.step.IStep;
import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Modifier;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            new FastClasspathScanner("com.merce.woven", "com.inforefiner.rtcflow.steps.sink.hbase").matchClassesImplementing(IStep.class,
                    aClass -> {
                        if (!Modifier.isAbstract(aClass.getModifiers())) {
                            try {
                                StepBind stepAnnotation = aClass.getAnnotation(StepBind.class);
                                String xtype = stepAnnotation.id();
                                System.out.println("step class loaded: " + xtype);
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.err.println("load step " + aClass.getName() + " failed: " + e.getMessage() + ", ignored.");
                            }
                        }
                    }
            ).scan();
            Thread.sleep(100);
        }

    }

}
