bgen="../data_test/samp_100_var_100000.bgen"
./bgenix -g $bgen -index

hyperfine "../target/release/bgen_reader -f $bgen"
hyperfine "./bgenix -g $bgen -list"

