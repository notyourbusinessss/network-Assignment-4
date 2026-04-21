# CS 464/564 – Assignment 4: Replicated Bulletin Board Server

## server side
The entire thing cna be simply compiled with 
```sh
make
```

after this simply run 

```sh
./rbserv
```

now on the client side you can 

## client side 

the client can be nc or telnet. run it with for example if the ip of the server is 192.168.1.2, here well use telnet so run ;
```sh
telnet 192.168.1.2 7777
```

after this the server should greet you, like such :
```cpp
"0.0 WELCOME ver 1.0: USER READ WRITE REPLACE QUIT spoken here"
```

you should then use the user command to tell the server who you are

like : 

```sh
USER George
```
The the server is gonna respond by welcoming you with your name

```cpp
"1.0 HELLO " + name + " welcome"
```

After this you can do the following commands
### Commands

READ : Read takes an argument which should be an id like so : 
```sh 
READ <ID>
```



