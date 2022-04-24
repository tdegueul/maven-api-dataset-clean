package mcr;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class ClassStatisticsVisitor extends ClassVisitor {
	private int classVersion;
	private int methods;
	private int fields;
	private int publicMethods;
	private int publicFields;
	private int protectedMethods;
	private int protectedFields;
	private String myName;
	private boolean pblic;
	private boolean intrface;
	private boolean abstrct;
	private boolean anonymous;
	private boolean enm;
	private boolean isJava    = false;
	private boolean isScala   = false;
	private boolean isGroovy  = false;
	private boolean isClojure = false;
	private boolean isKotlin  = false;

	public ClassStatisticsVisitor() {
		super(Opcodes.ASM5);
	}

	@Override
	public void visitSource(String source, String debug) {
		if (source != null) {
			if      (source.endsWith(".java"))   isJava    = true;
			else if (source.endsWith(".scala"))  isScala   = true;
			else if (source.endsWith(".groovy")) isGroovy  = true;
			else if (source.endsWith(".clj"))    isClojure = true;
			else if (source.endsWith(".kt"))     isKotlin  = true;
		}
	}

	@Override
	public void visit(final int version, final int access, final String name, final String signature,
			final String superName, final String[] interfaces) {
		myName = name;
		classVersion = version;
		pblic = (access & Opcodes.ACC_PUBLIC) != 0;
		intrface = (access & Opcodes.ACC_INTERFACE) != 0;
		abstrct = (access & Opcodes.ACC_ABSTRACT) != 0;
		enm = (access & Opcodes.ACC_ENUM) != 0;
	}

	@Override
	public MethodVisitor visitMethod(final int access, final String name, final String descriptor,
			final String signature, final String[] exceptions) {
		methods++;
		if ((access & Opcodes.ACC_PUBLIC) != 0)
			publicMethods++;
		if ((access & Opcodes.ACC_PROTECTED) != 0)
			protectedMethods++;
		return null;
	}

	@Override
	public FieldVisitor visitField(final int access, final String name, final String descriptor, final String signature,
			final Object value) {
		fields++;
		if ((access & Opcodes.ACC_PUBLIC) != 0)
			publicFields++;
		if ((access & Opcodes.ACC_PROTECTED) != 0)
			protectedFields++;
		return null;
	}

	@Override
	public void visitInnerClass(String name, String outer, String innerName, int access) {
		if (name.equals(myName)) {
			anonymous = (innerName == null);
		}
	}

	public int getClassVersion() {
		return classVersion;
	}

	public int getMethods() {
		return methods;
	}

	public int getFields() {
		return fields;
	}

	public int getPublicMethods() {
		return publicMethods;
	}

	public int getPublicFields() {
		return publicFields;
	}

	public int getProtectedMethods() {
		return protectedMethods;
	}

	public int getProtectedFields() {
		return protectedFields;
	}

	public boolean isAnonymous() {
		return anonymous;
	}

	public boolean isPublic() {
		return pblic;
	}

	public boolean isInterface() {
		return intrface;
	}

	public boolean isAbstract() {
		return abstrct;
	}

	public boolean isEnum() {
		return enm;
	}

	public boolean isClass() {
		return !isInterface() && !isEnum();
	}

	public boolean isJava() {
		return isJava;
	}

	public boolean isScala() {
		return isScala;
	}

	public boolean isGroovy() {
		return isGroovy;
	}

	public boolean isClojure() {
		return isClojure;
	}

	public boolean isKotlin() {
		return isKotlin;
	}
}
