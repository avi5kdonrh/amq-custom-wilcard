# amq-custom-wilcard

Run the CustomWildcardTest to see that setting an any-word wildcard to $ shows unexpected logs

```declarative
 mvn test -Dtest=CustomWildcardTest
```

On the contrary, the DefaultWildcardTest shows that the default setting for any-word (#) works fine and doesn't 
cause any issues.

```declarative
mvn test -Dtest=DefaultWildcardTest
```