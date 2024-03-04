bgen="../data_test/samp_100_var_100000.bgen"
hyperfine -p "rm $bgen.bgi" "./bgenix -g $bgen -index"
hyperfine -p "rm $bgen.bgi_rust" "../target/release/bgen_reader -f $bgen index"

# hyperfine "../target/release/bgen_reader -f list"
# hyperfine "./bgenix -g $bgen -list"

