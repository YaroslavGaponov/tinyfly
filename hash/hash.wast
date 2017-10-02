(module
    (memory $0 1)
  
    (export "memory" (memory $0))
    (export "hash" (func $hash))
    
    (func $hash (param $seed i32) (result i32)
      ;; i = 0
      (local $i i32)
      
      ;; result = 0
      (local $result i32)
      
      ;; result = seed;
      (set_local $result (get_local $seed))
      
      (block $break
        (loop $continue
        
        
        (br_if $break (i32.eqz (i32.load8_u (get_local $i))))
        
        (set_local $result
          (i32.add
            (i32.sub
                (i32.shl (get_local $result) (i32.const 5))
                (get_local $result)
            )
            (i32.load8_u (get_local $i))
          )
        )
        
        ;; i++
        (set_local $i (i32.add (get_local $i) (i32.const 1)))
        
        (br $continue)
        
        )
      )
      
      (get_local $result)
    )
)