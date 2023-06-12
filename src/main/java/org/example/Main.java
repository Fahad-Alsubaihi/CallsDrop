package org.example;
public class Main {
    public static void main(String[] args) throws InterruptedException {

        DropCallsProducer callDropspro =new DropCallsProducer("employees","key1");
        callDropspro.start();
//        to make sure the topic created before creating consumer
        Thread.sleep(5000);
        System.out.println("Wake up");
        DropCallsConsumer callDropscons=new DropCallsConsumer("employees",0,0);
        callDropscons.start();
    }
}