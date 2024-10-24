# bgen="../data_test/samp_100_var_100000.bgen"
bgen="../HG_files/1000G_100k.bgen"
bgi="$bgen.bgi"
bgi_rust="$bgen.bgi_rust"

generate_bgi="./bgenix -g $bgen -index"
generate_bgi_rust="../target/release/bgen_reader -f $bgen index"

if [ ! -f $bgi ]; then
  $generate_bgi
fi
if [ ! -f $bgi_rust ]; then
  $generate_bgi_rust
fi

# Benchmarking index file creation

# hyperfine -p "rm $bgi" "$generate_bgi"
# hyperfine -p "rm $bgi_rust" "$generate_bgi_rust"

# # Benchmarking listing of variants

hyperfine "./bgenix -g $bgen -list"
hyperfine "../target/release/bgen_reader -f $bgen list"

# Benchmarking vcf file
# hyperfine -r 1 "./bgenix -g $bgen -vcf > bgenix.vcf"
# hyperfine -r 1 "../target/release/bgen_reader -f $bgen vcf"

