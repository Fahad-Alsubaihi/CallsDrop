package org.example;

import java.util.ArrayList;
import java.util.Random;


public class People_list {

   public ArrayList<People> pl;
    People_list(){

        pl=new ArrayList<>(10);

        pl.add(new People("00001","Fahad","Developer"));
        pl.add(new People("00002","Nawaf","Developer") );
        pl.add(new People("00003","Ali","Developer") );
        pl.add(new People("00004","Reem","Developer") );
        pl.add(new People("00005","Rafeef","Developer") );
        pl.add(new People("00006","Lamees","Developer") );
        pl.add(new People("00007","Saleh","Developer") );
        pl.add(new People("00008","waad","Track owner") );



    }
    People getrand(){
        Random rand = new Random();
        return pl.get(rand.nextInt(pl.size()));
 
    }






}
class  People{
    String phone;
    String name;
    String position;
    People(String phone,String name,String position){
        this.name=name;
        this.position=position;
        this.phone=phone;
    }
}

