echo "Testing merging times"
list="../data_test/list"
input_file="../data_test/samp_100_var_100000.bgen"
../target/release/bgen_reader --filename "$input_file" list rsid >"$list"
list1="../data_test/list_p1.txt"
list2="../data_test/list_p2.txt"
head -n50000 $list >"$list1"
tail -n50000 $list >"$list2"
first_half="../data_test/samp_100_var_50000_p1.bgen"
second_half="../data_test/samp_100_var_50000_p2.bgen"
../target/release/bgen_reader --filename "$input_file" bgen --incl-rsid-file $list1 $first_half
../target/release/bgen_reader --filename "$input_file" bgen --incl-rsid-file $list2 $second_half

tmp_merge="tmp.merge"
echo "$first_half" >"$tmp_merge"
echo "$second_half" >>"$tmp_merge"
out="../data_test/reconstructed.bgen"

# bgen_reader
hyperfine "../target/release/bgen_reader --filename $first_half merge $tmp_merge $out"

# qctool
samples="../data_test/100.sample_qctool"
hyperfine "./qctool -g $first_half -s $samples -merge-in $second_half $samples -og $out"

# rm $list $list1 $list2 $first_half $second_half $tmp_merge
