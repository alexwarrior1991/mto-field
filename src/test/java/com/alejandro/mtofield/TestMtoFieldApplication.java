package com.alejandro.mtofield;

import org.springframework.boot.SpringApplication;

public class TestMtoFieldApplication {

    public static void main(String[] args) {
        SpringApplication.from(MtoFieldApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
