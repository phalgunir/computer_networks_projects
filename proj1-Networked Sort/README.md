# **Project 1 : Distributed Sort**

Testing Commands : 
<br />cp testcases/testcase2/input-0.dat INPUT
<br />cat testcases/testcase2/input-1.dat >> INPUT && cat testcases/testcase2/input-2.dat >> INPUT && cat testcases/testcase2/input-3.dat >> INPUT && cat testcases/testcase2/input-4.dat >> INPUT && cat testcases/testcase2/input-5.dat >> INPUT && cat testcases/testcase2/input-6.dat >> INPUT && cat testcases/testcase2/input-7.dat >> INPUT
<br />
<br />
<br />cp testcases/testcase2/output-0.dat OUTPUT
<br />cat testcases/testcase2/output-1.dat >> OUTPUT && cat testcases/testcase2/output-2.dat >> OUTPUT && cat testcases/testcase2/output-3.dat >> OUTPUT && cat testcases/testcase2/output-4.dat >> OUTPUT && cat testcases/testcase2/output-5.dat >> OUTPUT && cat testcases/testcase2/output-6.dat >> OUTPUT && cat testcases/testcase2/output-7.dat >> OUTPUT
<br />
<br />
<br />utils/m1-arm64/bin/showsort INPUT | sort > REF_OUTPUT
<br />utils/m1-arm64/bin/showsort OUTPUT > my_output
<br />diff REF_OUTPUT my_output