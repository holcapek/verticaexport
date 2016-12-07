SELECT (COUNT(0) = 1)
FROM   user_libraries
WHERE  user_libraries.lib_name = 'unload_lib'
-- AND    (user_libraries.md5_sum = '3d990613eb18f24bccd6035b0530da6a' or public.length('3d990613eb18f24bccd6035b0530da6a') = 7);
;
