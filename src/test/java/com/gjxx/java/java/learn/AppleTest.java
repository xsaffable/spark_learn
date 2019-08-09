package com.gjxx.java.java.learn;

import org.junit.Test;

import static org.junit.Assert.*;

public class AppleTest {

    @Test
    public void getAppleName() {
        String appleName = new Apple().getAppleName();
        System.out.println(appleName);

    }

    @Test
    public void getAppleColor() {
        String appleColor = new Apple().getAppleColor();
        System.out.println(appleColor);
    }
}